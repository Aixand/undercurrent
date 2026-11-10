# Control sessions through CLI

Read this reference after selecting CLI mode, or for a deliberate CLI handoff
allowed by the entrypoint's automatic-mode rules. Keep the selected machine,
OS user, and home. Check `uc --version` and relevant command help; these
instructions target matching 0.1.0-beta native builds. For missing or incompatible
setup, read [CLI setup](setup.md). Version/help checks do not connect; ordinary
session reads can auto-start a daemon. Use configuration validation and daemon
status for offline diagnosis.

## Target and operate

CLI positional targets are exact names. Retain stable IDs from launch results.
On resume, query `uc ls --id SESSION_ID --json`, recover the current name, then
compare `uc inspect NAME --json`'s `summary.id` with the recorded ID. These checks
are not atomic guards against concurrent rename/removal. Do not act on a reused
name or assume an ID works as a positional selector.

| Goal | Command pattern |
| --- | --- |
| Run a program | `uc start NAME --cwd DIRECTORY --json -- PROGRAM ARG...` |
| Keep a shell available | `uc start NAME --interactive --cwd DIRECTORY --json` |
| Inspect current state | `uc inspect NAME --json` |
| Inspect the visible terminal | `uc peek NAME` |
| Read retained output | `uc logs NAME --max-bytes 32768 --json` |
| Type into the existing program | `uc send NAME "TEXT" --json` |
| Run a separate command in launch context | `uc exec NAME --dir-launch --env-launch -- PROGRAM ARG...` |
| Wait for a finite job | `uc wait NAME --lifecycle exited --lifecycle failed --lifecycle stopped --timeout 30s --json` |
| Request a graceful stop | `uc stop NAME --graceful --json` |

Replace names, directories, and argv elements before executing. `start` and
`exec` do not parse shell expressions. Invoke a shell explicitly for pipelines,
redirection, or builtins. `exec` is text-only; do not add `--json`. It starts a
separate process, while `send` delivers terminal input.

Read [CLI session workflows](sessions.md) for finite jobs, servers, input,
attachment, execution context, and cleanup. `send` normally adds Enter after a
nonempty body; use `--no-newline` when that is unwanted. Use `attach` only with a
real interactive terminal.

## Observe and recover

Read [CLI observation](observation.md) for command-specific result shapes, waits,
output pagination, and stream limits. Keep stdout and stderr separate. Retain
`session.id` from a start. CLI wait has no revision floor and can match initial
state; confirm new application evidence after input. Continue output at
`to_offset`, not the producer head `next_offset`. List cursors do not survive a
separate CLI invocation; query recorded IDs individually when recovering workers.

Read [CLI recovery](recovery.md) after uncertain effects, before any retry.
Missing responses and transport failures can follow successful mutations.
Preserve chosen delivery IDs with exact payloads; changing interfaces does not
make replay safe.

Graceful stop can escalate after its configured timeout. Default stop/restart is
immediate. Inspect terminal lifecycle and restart intent after stopping, then
retain needed output before `uc clean NAME --yes --json`. Scope cleanup to the
intended sessions and leave shared daemons running.
