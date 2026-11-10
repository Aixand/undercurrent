# Launch finite or interactive workers

Use the user's chosen, installed agent CLI. Check its local help for prompt,
working-directory, permission, and output-file options. Each worker needs its
own task context, agent authentication, and accessible working directory. Preserve
the selected model and permissions; no bypass flags are needed by this workflow.

## Prepare paths and a task

Preparation needs access to files on the worker's machine in either interface
mode. MCP supplies session control, not task-file or worktree creation. Use
available file/Git/shell tools within the task's scope. This example uses a POSIX
shell and an existing Git project; on Windows use native paths and PowerShell.

From the project root, for a **new** run:

```sh
uc_project="$PWD"
uc_run_dir="$(mktemp -d /tmp/uc-orchestration.XXXXXX)"
uc_run_id="${uc_run_dir##*/}"
uc_worker_name="${uc_run_id}-architecture"
```

Copy the [manifest template](../assets/run-manifest.json) to `$uc_run_dir/run.json`, fill the
run fields, and record this worker before launching. Persist all generated paths;
on resume, recover them from the manifest instead of running this setup again.
Use persistent storage instead of `/tmp` if results must survive its cleanup.

Write `$uc_run_dir/architecture-task.md` with a concrete assignment, for example:

```text
Review the architecture of the supplied project. Identify up to five actionable
issues, with file/line evidence, impact, and a recommended correction. Explain
which areas you examined and any limits. Do not change project files or spawn
other agents. Return the review as your final message, including the run and
task identifiers supplied by the parent. State explicitly if there are no findings.
```

Add the actual run/task identifiers, relevant requirements, and scope. This is
a task specification supplied to the worker, not an Undercurrent command.

## Choose the worker's lifetime

For a finite task, use the selected agent's supported non-interactive invocation
and final-message export when available. Give it the task as an argv element or
accessible file path; redirecting input into a session-launch command is not
equivalent. Use an unused output path and record worker mode `finite`.

For continued conversation, launch the agent's interactive command and record
mode `interactive`. Inspect its foreground UI before delivering a task. Automatic
profile detection can identify supported prompts; unknown detection needs screen
inspection. Resolve startup questions within existing authorization. Sending
text into an approval dialog is not task submission.

For each interactive turn, require a writable, unique result path tied to the
run/task/turn. Record the exact input, delivery identity, and intended result
before sending. Preserve each receipt and foreground revision separately from
aggregate session revisions. Never overwrite earlier turn evidence.

## Launch through the selected interface

Read only the matching examples:

- [CLI worker launches](launching-workers-cli.md) for CLI mode.
- [MCP worker launches](launching-workers-mcp.md) for MCP mode.

Both examples use Codex after checking its local help. Keep another selected
agent/model/permission configuration instead when the user specified it. Review
workers can use read-only project access; implementation workers need the
authorized write permissions and their assigned isolated directory.

Launch independent workers with distinct task files, names, and result paths
before waiting. Inspect each finite worker's exit evidence and fresh final result;
a successful state wait can still observe failure. Timeouts leave workers alive.
For interactive workers, require new task-specific output and validate its
artifact; an idle state can predate input and the process may stay alive after a
completed turn. Do not send follow-up text into a finished process. Reconcile the
existing attempt before using a new task or documented conversation-resume path.
