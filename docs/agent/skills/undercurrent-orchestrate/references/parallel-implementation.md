# Parallel implementation and dependent tasks

Partition work along interfaces or file ownership. Agree on shared interfaces
before concurrent implementation. Use independent worktrees for workers whose
changes could overlap; separate directories are sufficient only when their
outputs and tools are actually independent. Schedule tightly coupled edits in
sequence instead of manufacturing concurrency.

## Prepare isolated work

Inspect `git status` and record the intended baseline. New worktrees contain
committed content, not the user's uncommitted changes. If tasks depend on those
changes, prepare an explicit reviewed snapshot or serialize work in the intended
checkout. Do not stash, reset, or discard the user's work implicitly.

For a clean committed baseline, this POSIX example runs from the project root
after the run variables have been created as in [launching workers](launching-workers.md):

```sh
uc_base_commit="$(git rev-parse HEAD)"
git worktree add -b "uc/$uc_run_id/api" "$uc_run_dir/api-worktree" "$uc_base_commit"
git worktree add -b "uc/$uc_run_id/tests" "$uc_run_dir/tests-worktree" "$uc_base_commit"
```

Use unique, recorded branch and path names. Assign one worktree to each worker
and select it as the working directory in that interface's launch request.
Prepare necessary dependencies using the
project's existing setup workflow. Worktrees isolate files, not service ports,
shared databases, credentials, or global caches; give runtime fixtures distinct
resources where needed.

## Task and result contract

Specify owned paths, shared interfaces, acceptance criteria, and verification
commands. A worker should return changed files, rationale, checks actually run,
their results, and known limitations. State whether committing is part of its
assignment. Do not assume an uncommitted change will arrive as a commit.

A completed worker is ready for parent review. Inspect its diff and checks before
accepting the task. Preserve both tracked changes and new files if moving work
between directories; a plain tracked diff omits untracked files.

Integrate reviewed changes using the project's chosen Git workflow and existing
authorization. Do not blindly cherry-pick every worker commit or discard a
conflicting patch. Resolve overlapping intent, then run the relevant checks on
the combined result. Keep another worker's active worktree unchanged during this
step. Do not force-remove a dirty worktree or delete branches holding needed work.

## Ordered tasks

For a queue or dependency graph, record task IDs and prerequisites in the manifest.
After a prerequisite finishes, verify and integrate its result before preparing
the dependent worker's baseline. Launching every worktree at the original HEAD
would hide newly completed prerequisites from dependent workers.

If a task fails, block only its dependents; unrelated authorized work may continue.
On timeout or interrupted orchestration, reconcile its existing session and
working tree before retrying. A retry gets a new task-attempt identity and output
path only after the earlier attempt's effects are understood.

When finished, retain the integrated changes and verification evidence. Stop
run-owned workers as appropriate. Remove a worktree only after its process is
terminal and its changes are integrated or otherwise preserved.
