# Start, control, and clean up a session

Run `uc` from your project directory. These tasks assume it is already installed
and use your current Undercurrent home.
A **session** is the daemon's stable record for supervised work. Its unique name
is convenient for CLI lookup; its ID survives rename and process restarts.

## Choose the kind of work

| Goal | Command pattern | What success means |
| --- | --- | --- |
| Run one program | `uc start NAME -- PROGRAM ARG...` | Creation and activation returned a result; the program may still run. |
| Keep an interactive shell | `uc start NAME --interactive --attach` | A shell was started and a terminal opened. |
| Type into the existing terminal | `uc send NAME "TEXT"` | Input delivery was handled, not that the child finished processing it. |
| Run another program in session context | `uc exec NAME -- PROGRAM ARG...` | A separate process returned a finite execution result. |
| Wait for a condition | `uc wait NAME --lifecycle exited --timeout 30s` | One observed summary matched. Timeout does not stop the session. |

Replace uppercase placeholders with your values. `PROGRAM ARG...` is an argument
vector, not an implicitly parsed shell expression. For pipelines, redirects, or
shell builtins, explicitly invoke your shell's command option. The complete
[first-session tutorial](../../getting-started/first-session.md)
demonstrates this on POSIX and Windows with isolated state and cleanup.

## Work in an interactive shell

Follow [open and leave a session](interactive.md#open-and-leave-a-session) to
create `dev`, detach, verify it remains, and reattach. That guide also covers
read-only viewing, panes, and their terminal-size effects.

To run another process in `dev`, use `uc exec dev -- PROGRAM ARG...`. The default
live directory and root-shell exports require available current context.
[Install shell hooks](shell-integration.md#preview-install-and-verify) for export
capture, or choose `--dir-launch` and/or `--env-launch` deliberately.
[Exec reference](../../reference/cli-commands.md#run-a-separate-command-with-exec)
explains result validation and exit codes.

## Send input deliberately

From a POSIX caller shell, send a command to the interactive `dev` session:

```sh
uc send dev 'printf "input received\n"'
uc logs dev --max-bytes 4096
```

The default send appends Enter. Look for `input received` in the shell output;
a delivery result alone does not prove the application processed the input.
Use [send framing and key options](../../reference/cli-commands.md#deliver-input-or-run-a-separate-command)
for paste, literal text, keys, stdin, and close-standard-input behavior.

A transport failure can leave delivery unknown. Inspect output and application
acknowledgments before sending another copy. A delivery ID is scoped to the
[daemon input contract](../../reference/cli-commands.md#deliver-input-or-run-a-separate-command);
it does not make every CLI mutation safe to repeat across daemon replacement.

## Stop, restart, and remove

Run `uc stop` in a terminal to pick a session with Up/Down and stop it with Enter.
Escape cancels. Stop and restart terminate the old process immediately by default.
Use `uc stop --graceful` or `uc restart NAME --graceful` to send the configured
stop signal and allow the session's stop timeout before escalation. `--force`
remains an explicit spelling of the immediate default. The picker also includes exited sessions with a
scheduled restart, so you can cancel that restart. For scripts, supply a name,
`--all`, or `--label`; see [selection rules](../../reference/cli-commands.md#start-stop-restart-and-remove-sessions).

To finish the `dev` example:

```sh
uc stop dev
uc inspect dev --json
uc clean dev --yes
```

Verify a terminal lifecycle before cleanup. If stop fails or times out, preserve
the record and [reconcile the outcome](../troubleshooting.md). Failure to receive a
reply does not prove failure to deliver a signal. `clean` refuses active sessions;
without a name it selects terminal sessions. Automation needs explicit `--yes`.

An unconditional immediate stop can interrupt an ongoing graceful stop of the
same process. A timeout only ends the CLI wait; it does not cancel the accepted
stop. Revision-conditional SDK stops retain their serialized precondition check.

`uc restart NAME` uses saved configuration and intentionally starts another
process incarnation. Automatic restart is separately selected at start with
`--restart on-failure|always` and a bounded policy. Read [lifecycle](../../architecture/lifecycle.md)
before interpreting a terminal-looking session with scheduled restart.
`--rm` requests removal on completion and can eliminate later inspection evidence.

For bulk stop or cleanup, read [selection and partial results](../../reference/cli-commands.md#start-stop-restart-and-remove-sessions)
before running the command. Successful changes remain when other members fail;
inspect the failure report before further action.

## Distinguish the results

- [Save a group of shells](groups.md): restore recipes and reconcile pending operations.
- [Inspect state and output](status-and-output.md): determine what the program produced.
- [Understand start, readiness, and success](../../architecture/lifecycle.md#distinguish-start-readiness-and-success): use the appropriate evidence for each result.

Closing a pane or subscription leaves the program running. If you created a
dedicated daemon home, stop its daemon after finishing all work. Leave a shared
daemon available for other sessions.

[Read status and output](status-and-output.md) · [Command reference](../../reference/cli.md)
