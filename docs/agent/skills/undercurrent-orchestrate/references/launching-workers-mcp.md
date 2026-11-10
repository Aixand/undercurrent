# Launch workers through MCP

Use this in selected MCP mode after [preparing the task](launching-workers.md)
and checking [MCP control](mcp-control.md). Replace example names, paths, and IDs
with recorded values on the MCP server's machine. Tool arguments are JSON values,
not shell source: shell variables and redirects are not expanded. Keep the
user's chosen agent/model/permissions; the example below is a Codex review worker
after checking its installed `exec --help`.

## Finite worker

Record `launching`, then call `start_session` with:

```json
{
  "name": "UNIQUE_WORKER_NAME",
  "program": "codex",
  "arguments": [
    "exec", "--sandbox", "read-only", "--color", "never",
    "--output-last-message", "/absolute/run/architecture-result.md",
    "Read the task in /absolute/run/architecture-task.md and complete it."
  ],
  "cwd": "/absolute/project",
  "labels": {"uc-run": "RECORDED_RUN_ID"}
}
```

MCP uses restart policy `never` and retains terminal records. Save the returned
`data.session.id` and full receipt. Start the other independent workers before
calling `wait_for_state` for each:

```json
{
  "session_id": "RECORDED_SESSION_ID",
  "lifecycle": ["exited", "failed", "stopped"],
  "timeout_seconds": 30
}
```

Inspect with `inspect_session` and the same ID. Read
`data.summary.lifecycle.state` and its available `code`; then use `read_output`
with a byte bound and continue at `data.to_offset` when needed. Read the fresh
result file with available file tools and verify task identity and substance.
No wait outcome or zero process exit establishes that the review itself is valid.

## Interactive worker

Record a distinct worker with mode `interactive`. Call `start_session` with the
chosen interactive program, such as `codex`, its intended arguments, absolute
cwd, unique name, and run label. Save its ID, then inspect state and `read_screen`
until the foreground task composer is ready. Resolve startup prompts within the
existing authorization before task delivery.

Use file tools to read the prepared task and put its actual contents into the
`paste.text` value below. Save a new delivery identity, the exact ordered items,
turn ID, and fresh result path before calling `send_input`:

```json
{
  "session_id": "RECORDED_SESSION_ID",
  "delivery_id": "NEW_RECORDED_DELIVERY_ID",
  "items": [
    {"kind": "paste", "text": "ACTUAL_TASK_FILE_CONTENTS"},
    {"kind": "key", "key": "enter"}
  ]
}
```

Use paste for a composer that understands bracketed paste; choose `kind: "text"`
for a plain stdin reader to avoid sending literal terminal control sequences.
Neither alone adds Enter. Bound the encoded arguments to the tool's limit;
for a large task, send a concise instruction referencing an accessible task file
instead of blindly splitting a submission. When a revision guard is appropriate,
set `expected_revision` to the integer `data.summary.revision` from inspection.
A stale guard requires fresh inspection and reconciliation, not removing the guard
to force delivery. Keep the receipt's `foreground_revision` separately.

Use bounded state/output observations while looking for new task-specific
evidence. A `min_revision` on a wait is an inclusive aggregate-revision floor;
the receipt's foreground revision is not interchangeable with it. An idle match
still does not prove the turn completed. Validate the fresh result artifact.

## Stop and preserve evidence

For a run-owned worker being stopped on Linux/macOS, call `stop_session` with its
ID and `mode: "graceful_only"`, then inspect and wait for terminal state. This
mode does not force escalation. On Windows it is rejected before submission;
use application-specific shutdown or deliberately select `graceful_then_force`
or `force` when appropriate for the task. Preserve results and output. Explicit MCP mode
leaves terminal records in place because record deletion is outside its tools.
