# Repository file policy

Coding-assistant repository instructions and configuration must not be tracked.
The rule applies at every directory level, case-insensitively, including symlinks.
The authoritative patterns are in
[`check-repository-files.py`](scripts/checks/check-repository-files.py).

Recognized paths include `AGENTS.md`, `AGENTS.override.md`, `CLAUDE.md`,
`CLAUDE.local.md`, `CODEX.md`, `GEMINI.md`, `QWEN.md`, `AMP.md`, `WARP.md`,
`CRUSH.md`, `OPENCODE.md`, AI context/instruction files, Copilot instructions,
OpenCode configuration, and Cursor, Windsurf, Cline, Roo, Continue, Aider,
Gemini, Codex, Claude, Junie, and other listed assistant configuration directories.
`.github/agents/`, `.github/instructions/`, and `.github/prompts/` are also excluded.

The check examines path names. Application code, ordinary documentation, prompt
packs, and actual product assets such as a skill package's `SKILL.md` remain
allowed. Mentioning an assistant or demonstrating an instruction filename in
documentation or a test string does not introduce a forbidden tracked file.
Add new assistant filenames to the shared checker and its regression tests when
the policy needs to cover another tool.

## Before committing

Run from the repository root with Python 3:

```sh
python3 scripts/checks/install-file-policy-hook.py
python3 scripts/checks/check-repository-files.py
```

The installer adds a small pre-commit hook only in the current clone. It checks
the complete Git index, so an existing tracked file is rejected even when it is
unstaged, missing from disk, or hidden by `.gitignore`. Stage its removal before
retrying. Ignored, untracked local files do not enter the proposed commit and
are outside this check. Ignore rules are a convenience; forced additions are
still rejected.

Existing hook managers and custom hooks are preserved. If one is present, add
the checker to that manager instead of replacing it. For pre-commit:

```yaml
- repo: local
  hooks:
    - id: repository-file-policy
      name: Reject coding-assistant repository files
      entry: python3 scripts/checks/check-repository-files.py
      language: system
      stages: [pre-commit, pre-push, manual]
      pass_filenames: false
      always_run: true
```

Use the project's available Python executable on Windows. The standalone
installer records the interpreter used to run it; reinstall the hook if that
interpreter is removed. To remove only the managed hook, delete this clone's
`.git/hooks/pre-commit` after checking its ownership comment. Cloning does not
install hooks; each maintainer installs or integrates the check in their clone.

## CI and history

The **Repository file policy** job checks the current tracked tree and every
introduced commit on PRs or noninitial pushes. An add-then-delete sequence cannot
hide a forbidden file in an intermediate commit. The standalone range command is:

```sh
python3 scripts/checks/check-repository-files.py --base BASE_COMMIT --head HEAD_COMMIT
```

For a forced push, CI checks every commit reachable from the replacement head.
It does not require the removed history to exist in the checkout. Run that check
locally with:

```sh
python3 scripts/checks/check-repository-files.py --all-history
```

Local hooks can be skipped; require the successful CI job through the project's
normal GitHub-settings setup when its prerequisites are satisfied. Installing
files or a hook alone does not enable branch protection. The new policy does not
rewrite existing history: it removes currently tracked assistant files and guards
future commits after adoption.
