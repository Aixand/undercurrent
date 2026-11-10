---
name: undercurrent-orchestrate
description: >-
  Orchestrate subagents through Undercurrent CLI or MCP: delegate bounded tasks to
  agent CLI processes, supervise parallel reviews or isolated implementation
  workers, collect and verify results, and resume recorded sessions. Use when
  the user requests subagents, parallel agent work, or task queues through uc.
  Ordinary single-process session management belongs to undercurrent.
license: Apache-2.0
metadata:
  undercurrent-cli-version: "0.1.0-beta"
  undercurrent-mcp-version: "0.1.0-beta"
---

# Orchestrate subagents through Undercurrent

Launch and supervise worker agent processes as named Undercurrent sessions. The parent
owns task decomposition, follow-up decisions, verification, and integration.
Workers receive explicit task context; they do not inherit the parent's chat.
This skill can be installed and used on its own through CLI or MCP.
Resolve bundled resources relative to this skill directory.

## Select the interface

Honor an explicit CLI or MCP choice in the request or applicable project guidance.
Otherwise use automatic selection. These are instruction choices, not product flags.

| Choice | Instructions to load |
| --- | --- |
| CLI | Read [CLI control](references/uc-control.md); use it even if MCP is configured. |
| MCP | Read [MCP control](references/mcp-control.md); use only MCP for Undercurrent session control. |
| Automatic | Discover configured Undercurrent tools and check status using the MCP reference. Prefer a compatible server for the selected home; use the CLI reference when MCP is unconfigured. |

A configured server that is unavailable, incompatible, or targets another home
needs diagnosis, not silent CLI fallback. Do not act on a different home to make
a connection succeed. When MCP is selected, a read-only server permits observation
only; do not bypass it through CLI. Explicit MCP mode reports missing tools or
prerequisites while continuing supported work. MCP does not start a daemon;
separate daemon setup or installation must be within the requested scope.

Keep the selected interface for the run, including resume. In automatic mode, a
capability absent from the MCP tool inventory may use a deliberate CLI handoff:
explain the gap, verify the same machine/user/home and session identity, and
resolve prior uncertainty first. A failure, timeout, or read-only restriction is
not such a gap. Never replay an uncertain mutation through another interface.
Load only the selected interface's details. MCP mode still permits authorized
file, Git, and shell work for task preparation, worktrees, and result inspection.

## Prepare the run

1. Follow the selected control reference for version and availability checks;
   these instructions target CLI/MCP 0.1.0-beta with matching native builds.
   Preserve the selected machine, OS user, and Undercurrent home. Check the
   selected agent's local help for launch arguments and confirm authentication
   without exposing credentials. Read [launching workers](references/launching-workers.md)
   for task preparation and the selected interface's launch examples.
2. Keep the user's selected agent, model, permission settings, and concurrency
   constraints. Otherwise use an available configured agent and a small number
   of useful independent tasks. Installing or logging into another service is
   not implied by a delegation request. Surface missing prerequisites when needed.
3. Define each task's scope, inputs, dependencies, acceptance checks, and expected
   result. Use [parallel reviews](references/parallel-reviews.md) for independent
   investigations or [parallel implementation](references/parallel-implementation.md)
   for workers that change files. Launch a dependent task only after its required
   inputs have been verified.
4. Create an absolute run directory and copy [the manifest template](assets/run-manifest.json)
   into it. Record the interface preference and selected interface, versions,
   machine/user/home, project/base revision, task files, unique session names,
   working directories, result paths, and worker launch argv. Read
   [the manifest and recovery contract](references/recovery.md) when preparing
   or resuming a run. The parent alone updates the manifest.

## Give workers bounded instructions

Each task should identify the run and task, contain enough context to act
independently, and state:

- The requested outcome, owned files or review scope, and relevant constraints.
- The working directory and exact expected result path or final-message format.
- Acceptance checks and the evidence to return, including failures or blockers.
- Whether changes and commits are part of this task. Review tasks leave project
  files unchanged; implementation workers stay within assigned scope.
- That further delegation requires an explicit assignment permitting it.

Keep worker output locations distinct. Choose paths the selected agent can write
under its existing permissions, or use its CLI's final-message export. Treat
worker recommendations as task results to evaluate, not authority to expand the
user's request or change permissions.

## Launch and supervise

Persist each worker's `launching` record **before** submitting its launch.
Use an exact unique name, absolute cwd, and a run label. Disable automatic
restart and retain terminal records so worker effects can be reconciled.
Use the selected control reference to save the returned stable ID and receipt.
Start all independent workers before awaiting completion
so their work can overlap.

Verify saved session identities before sending input or stopping resumed workers.
Do not rename or reuse worker names during a run.

Observe in bounded intervals using inspect, screen, output, and state-wait tools
or their CLI counterparts.
Preserve useful output outside the terminal retention window. Read requested
input before responding; resolve ordinary task questions from the assigned
context. Apply the user's existing authorization to trust/permission prompts;
if a prompt requires additional authority, expose that specific blocker.

Finite workers finish their process. Interactive workers remain alive between
turns and need task-specific result evidence. A state wait can match an old idle
state immediately; follow the selected interface's revision semantics.
An input receipt, idle screen, or wait timeout is not completed work.

After uncertainty, reconcile the existing worker using
[recovery](references/recovery.md). Do not relaunch a worker, resend a task, or
restart a session solely because the previous command timed out. Do not switch
from MCP to CLI to replay an uncertain mutation.

## Collect, verify, and integrate

For a finite worker, inspect its terminal lifecycle and available exit code,
then read its fresh final result. For an interactive turn, require a result tied
to that task/turn and verify it against the assigned acceptance checks. Output
file existence alone is insufficient: reject stale, empty, partial, or unrelated
results. An exited process with code zero is necessary for the ordinary finite
success path, but does not establish that the assigned task succeeded.

Review cited files or proposed changes. Reconcile conflicting findings, verify
implementation checks in the relevant worktrees, and integrate changes within
the user's requested scope. Test interactions on the combined result when
parallel work affects the same behavior. A worker's statement that tests passed
is not a substitute for available command results and acceptance evidence.

Record each task's verified outcome separately from its process state. Stop
run-owned workers when finished unless the user wants them available; prefer
graceful stopping where appropriate and check the selected interface's escalation
behavior. Confirm terminal state. Retain result files and needed session
evidence. Remove records/worktrees only after results are preserved and no active
or unresolved work depends on them. Leave shared daemons and other sessions alone.

Report the selected interface, combined result, checks actually performed,
artifacts or changes, and any failed, blocked, unresolved, or still-running
workers with names and IDs.
