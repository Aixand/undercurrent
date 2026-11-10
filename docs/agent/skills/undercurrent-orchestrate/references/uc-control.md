# Control workers through CLI

Read this only after selecting CLI mode, or for a deliberate CLI handoff allowed
by the entrypoint's automatic-mode rules. For worker examples, read
[CLI worker launches](launching-workers-cli.md) after [preparing the task](launching-workers.md).

Use the same machine, OS user, and resolved `UNDERCURRENT_HOME` throughout a run.
An explicit home must be absolute. Without an override, the default is
`$HOME/.aixand/undercurrent` on Unix or `%USERPROFILE%\.aixand\undercurrent` on
Windows. Record the resolved path in the manifest.

Check `uc --version`, `uc-daemon --version`, and `uc-session-host --version`;
these instructions target matching 0.1.0-beta builds. When building from source,
use matching revisions. `uc config validate` and `uc daemon status --json` do
not start a daemon. A nonzero status reporting that the daemon is not running is
expected for a new home. Ordinary session commands can auto-start an absent daemon.

The command patterns below use caller-shell placeholders. Replace `NAME`,
`DIRECTORY`, `RUN_ID`, and `AGENT ARG...` before executing.

| Operation | Pattern and result |
| --- | --- |
| Launch | `uc start NAME --cwd DIRECTORY --label uc-run=RUN_ID --restart never --json -- AGENT ARG...`; retain `session.id` and avoid `--rm`. |
| Inspect | `uc inspect NAME --json`; detail has `summary.id`, `summary.lifecycle`, and foreground observations. |
| Locate a saved ID | `uc ls --id SESSION_ID --json`; inspect the bounded page's `items` and obtain the current name. |
| Check run workers | `uc ls --label uc-run=RUN_ID --json`; one bounded page, not proof of a complete catalog. |
| Read current screen | `uc peek NAME`; screen text, not a full transcript. |
| Read retained bytes | `uc logs NAME --from-offset 0 --max-bytes 32768 --json`. |
| Wait for termination | `uc wait NAME --lifecycle exited --lifecycle failed --lifecycle stopped --timeout 30s --json`. |
| Observe input needs | `uc wait NAME --interaction requested --timeout 30s --json`, then inspect the prompt. |
| Send a task file | `uc send NAME --paste --delivery-id DELIVERY_ID --json < /absolute/task.md`; record the ID and exact payload first; body normally ends with Enter. |
| Send a key | `uc send NAME --key enter --json`; key-only input adds no extra Enter. |
| Stop | `uc stop NAME --graceful --json`; default stop without this flag is immediate. |
| Remove terminal record | `uc clean NAME --yes --json`; retain results first. |

CLI positional selectors are exact names, not stable IDs. Compare a resumed
name's `summary.id` with the saved ID. Between separate CLI calls this check is
not atomic; do not concurrently rename/remove workers. If such races require
atomic guards, report the CLI limitation rather than claiming a name check is one.

Inspect reads expose `summary.lifecycle.state` and, for an exit,
`summary.lifecycle.code` (possibly null). Read the actual response rather than
parsing human tables. A wait returns the first
matching summary, including an initial match, and does not return the child's
exit code as the CLI exit status. Inspect exit evidence separately. Alternatives
within one predicate are ORed; different predicate fields are ANDed.

`uc start` takes an argv, not shell source. Caller redirection into `uc start`
does not supply a task to the child agent. Pass its supported prompt argument,
give it a task-file path, or deliver input through `uc send` once its composer is
ready. `uc exec` starts a separate command, not a conversational follow-up. It is
text-only and needs captured live context unless `--dir-launch --env-launch` is
selected deliberately.

For logs, inspect `result`. A data page contains byte arrays and covers
`from_offset` through exclusive `to_offset`. Save bytes before advancing to
`to_offset`; `next_offset` is the producer head and can skip output. On a gap,
record missing history and choose a fresh screen or partial capture explicitly.
Reduce the bound if an encoded response exceeds limits. A follow stream can skip
unread bytes after its first seed page and need not close at process exit.

Query cursors cannot be continued across CLI invocations. For bounded worker
sets, query their recorded names/IDs individually. Report the CLI limitation if
full multi-page traversal is required. `attach` needs terminal stdin/stdout; ordinary
agent command tools should use finite reads and input delivery instead.

Failures appear on stderr. Preserve `effect_certainty` and other typed error
fields; a transport failure can leave a mutation applied or unresolved. Never
retry uncertain input just by retaining its delivery ID or switching interfaces.

Graceful stop can escalate after its configured timeout. Inspect terminal
lifecycle and restart intent afterward; a timeout does not cancel an accepted
stop. Preserve outputs before removing records, and leave shared daemons running.
