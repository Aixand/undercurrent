# Record and resume an orchestration run

The manifest is a parent-owned local JSON file, not daemon state or an accepted
control request. Copy the [bundled template](../assets/run-manifest.json) into the
run directory and fill the run fields with actual values. Use null for
`base_commit` when the project has no Git baseline and for an interface version
that was not used. Record relevant uncommitted baseline details separately.
Preserve the file until all work and cleanup outcomes are understood.

Use an atomic file replacement when saving updates. Only the parent edits the
manifest; workers write their own result files. Record intentions before effects
so a missing response can be reconciled after an interruption.

## Interface and target

Set `interface_preference` to `auto`, `cli`, or `mcp` according to the request or
applicable guidance; set `control_interface` to the selected `cli` or `mcp`.
These are manifest conventions, not flags or daemon settings. Record the resolved
machine, OS user, absolute home, and selected interface version. MCP-only runs
can leave `uc_version` null; CLI-only runs can leave `mcp_adapter_version` null.

Resume with the saved interface even when another interface has become available.
Honor a new explicit user choice only after verifying the same target and
reconciling pending effects. For a deliberate handoff allowed by the entrypoint,
retain the old choice and reason in `interface_history`, record the new choice,
and preserve the original interface on each launch/input receipt. Do not erase
pending operations or replay them because the interface changed. A temporary
handoff can restore the primary interface after its operation is reconciled.

Older manifests may lack the preference, target details, or history. Preserve
their worker records. Recover the recorded interface from existing receipts when
possible, verify missing target details against the actual environment, and fill
the new fields. If the original preference is unknown, use `auto` for future
selection; retain a known `control_interface` for the resumed run. A missing
field does not justify a new launch. If identity cannot be established, report
that specific unresolved state.

## Worker records

Add a record for each task/attempt. This example is illustrative; replace the
run-specific values with your actual task, paths, argv, and identities:

```json
{
  "task_id": "architecture",
  "attempt": 1,
  "depends_on": [],
  "mode": "finite",
  "session_name": "uc-review-unique-architecture",
  "session_id": null,
  "working_directory": "/absolute/project",
  "branch": null,
  "task_file": "/absolute/run/architecture-task.md",
  "result_file": "/absolute/run/architecture-result.md",
  "agent_argv": ["codex", "exec", "--sandbox", "read-only", "--output-last-message", "/absolute/run/architecture-result.md", "Read /absolute/run/architecture-task.md and complete it."],
  "status": "planned",
  "launch_interface": null,
  "start_receipt_file": null,
  "last_observed_lifecycle": null,
  "input_attempts": [],
  "verification": [],
  "cleanup": "not_requested"
}
```

Use orchestration status such as `planned`, `launching`, `running`,
`needs_input`, `ready_for_review`, `verified`, `failed`, `blocked`, or `unknown`.
These are parent classifications, separate from Undercurrent lifecycle. Save raw
tool/command receipts to distinct files and retain their paths. Do not put secrets in
the manifest or assume that an agent CLI's own conversation ID is a uc session ID.

Before a start, save `launching` with a unique name, actual argv, cwd, result
path, and `launch_interface`. Use the selected control reference to extract the
returned stable session ID and retain the receipt. A lost response leaves
`launching` or `unknown`, not permission to start another worker.

For interactive tasks, record each input attempt before delivery: a unique turn
ID, interface, delivery ID, exact payload (including submission keys), fresh
result path, and `pending` status. After sending, store the receipt path and
returned foreground revision if present. Keep any aggregate revision guard
separate from that foreground revision. A delivery identity does not authorize
replaying an unknown effect. Never overwrite earlier turn evidence with a new
turn's result.

## Resume procedure

1. Read the saved manifest; restore its interface, machine, OS user, home, and compatible version.
   Confirm recorded project and worktree paths still exist. Do not create a new
   run merely because the parent's conversation was interrupted.
2. Read only the selected [CLI control](uc-control.md) or [MCP control](mcp-control.md)
   reference. Verify availability and the recorded target, then recover and
   inspect each saved session ID using that interface's lookup procedure. If
   another process reused the old name, it is not this worker. Preserve receipts
   and uncertainty; changing interfaces does not make a retry safe.
3. For `launching` without an ID, inspect the original unique name and run label,
   then compare available process evidence and application artifacts against the
   planned launch before adopting it. Configuration/cwd may be available through
   CLI inspection; MCP intentionally excludes launch configuration. Do not invent
   unavailable projections or adopt a worker solely from a matching name.
   A bounded empty catalog or failed connection is not universal proof of no
   effect. Preserve uncertainty if the evidence cannot resolve the launch.
4. Read the current lifecycle, screen, retained output, result files, and worktree
   changes. A live worker may still be making progress or waiting for input.
   A terminal process still needs task-result verification. A missing session
   record does not erase changes it may already have made.
5. Resume observation or the next verified dependency. Do not automatically
   replay pending input or retry failed/unknown implementation work. Reconcile
   partial changes and existing task acknowledgments first.

## Failure handling

| Situation | Response |
| --- | --- |
| Wait timeout | Inspect and continue bounded observation if appropriate; the worker is still alive. |
| Missing result file | Check process state, agent errors, and task permissions; keep incomplete work distinct from success. |
| Lost send response | Inspect fresh application output/artifacts before any resend; terminal echo is not acceptance. |
| Agent exits nonzero | Save its output and partial changes; assess them before a new attempt. |
| Rate limit, login, or permission prompt | Use existing authorized setup or report the specific blocker; do not restart a loop of identical workers. |
| Stop timeout | Inspect the same session and pending restart intent; the stop may already be in progress. |
| Output gap | Record missing history, use current screen/artifacts where sufficient, and do not claim a complete transcript. |

Stop launching dependent tasks when their prerequisite outcome is unknown. Other
independent work can continue within the user's scope. Retry only after the
previous attempt is reconciled and the blocking cause is addressed; preserve the
old record and give the new attempt distinct names and output paths.

## Finish or hand off

Persist verified results, checks, and remaining blockers. Keep run-owned live
workers if continuation is intended; otherwise stop them deliberately with the
selected interface's appropriate graceful mode and confirm terminal state.
Archive useful output before any supported record deletion. Explicit MCP mode
can finish with retained terminal records; do not switch to CLI just to remove
them. Preserve worktrees holding unintegrated changes. Never
stop a shared daemon or remove its home to finish one orchestration run.
