# Observe state and output through CLI

Use this reference for CLI result shapes and observation. MCP mode uses its own
[control reference](mcp-control.md).

Use `--json` for finite commands and `--jsonl` for continuing streams. Keep
stdout and stderr separate. Results have command-specific shapes:

| Read | Relevant shape or meaning |
| --- | --- |
| `uc start NAME ... --json` | Mutation result with `session.id` and `session.name`. |
| `uc inspect NAME --json` | Detail with `summary.id`, `summary.lifecycle`, foreground observations, and optional process/configuration sections. |
| `uc ls --label uc-run=RUN_ID --json` | One bounded page with `items`, `next_cursor`, `snapshot_revision`, and optional `total`. |
| `uc explain NAME --json` | Evidence for foreground classification and configured health. |
| `uc peek NAME` | Current terminal screen as text, not a complete transcript. |
| `uc logs NAME --max-bytes 32768 --json` | Retained byte page with `result: data` or `result: gap`. |

In 0.1.0-beta, an inspected lifecycle is in `summary.lifecycle.state`; when it is
`exited`, `summary.lifecycle.code` is the exit code or null if unavailable.
Inspect the returned object instead of assuming `lifecycle` is a plain string or
that every successful CLI request returns a universal `success` wrapper.

`uc ls --id SESSION_ID --json` finds a known stable ID. Positional commands such
as `inspect`, `send`, `wait`, and `stop` resolve an exact **name**. If resuming a
known ID, obtain its current name and confirm `inspect.summary.id` before using
that name. Name-based checks are not atomic revision guards. If concurrent
rename/removal requires guarded control, report that CLI limitation rather than
treating a name check as a guard.

## Wait for the relevant evidence

`uc wait NAME --interaction available --interaction requested --timeout 30s --json`
matches either interaction state. Values within a field are ORed; different
fields are ANDed. Every wait can return from an already matching initial state.
CLI wait has no after-revision predicate. Use application acknowledgments or
task-specific result artifacts to establish that new input was handled.

A running process can have unknown execution; quiescence can mean blocked work.
Interaction requested requires examining the prompt, not automatically answering
yes. Health success covers only the configured checks. A wait timeout leaves the
process running. A request timeout is not a wall-clock deadline for all CLI
startup, input, and stream activity.

## Capture retained output

Start at offset 0 with an explicit byte bound. For each data page, verify
`from_offset`, save `bytes` (a numeric byte array), then continue at `to_offset`.
Require forward progress while `has_more` is true. `next_offset` is the producer
head and can skip unread retained bytes. Decode only after handling byte
continuity; a UTF-8 character can span pages.

For a gap, record the missing interval. Continue at `available_from` only when
incomplete history is acceptable. A screen snapshot can restore a view, not lost
history. Reduce `--max-bytes` and retry the same read offset if encoded response
limits reject a page; the default raw-byte bound can exceed the encoded limit.

`uc logs NAME --max-bytes 32768 --follow --jsonl` seeds one page and follows at
its producer head. It can skip an unread seed backlog. Use finite pagination
when collecting complete retained output. Follow need not end on process exit;
close observers when finished. Closing an observer leaves the process running.

## Bounded catalogs

List and watch cover a bounded page. A CLI query cursor cannot be continued in
a different invocation because cursors belong to the client connection. Inspect
recorded worker names individually or narrow the query. If complete multi-page
traversal is required, report that a sequence of CLI invocations cannot provide
it; do not silently change the selected interface.
