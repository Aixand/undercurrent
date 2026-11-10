# Run and control sessions through CLI

Use these examples only after selecting CLI under [CLI control](cli-control.md).

Examples use a POSIX caller shell. Replace example names with unused task-specific
names and run from the intended project directory. On Windows use the installed
program paths and PowerShell quoting; the `uc` command structure is the same.

## A finite job

```sh
uc start uc-example-job --restart never --json -- /bin/sh -c 'printf "job complete\n"'
uc wait uc-example-job --lifecycle exited --lifecycle failed --lifecycle stopped --timeout 30s --json
uc inspect uc-example-job --json
uc logs uc-example-job --max-bytes 4096 --json
```

Record `session.id` from start. Wait success means a selected terminal state was
observed, not that the job succeeded. Inspect the exit code and expected result.
Do not use `--rm` when later inspection or recovery needs the session record.

## A persistent development server

For an npm project whose `dev` script serves the specified health endpoint:

```sh
uc start uc-example-server --healthy-when 'http:http://127.0.0.1:3000/health' --json -- npm run dev
uc wait uc-example-server --health healthy --timeout 30s --json
uc inspect uc-example-server --json
```

Choose the actual command, port, and endpoint from the project. Do not invent a
health endpoint. A port rule such as `port:3000` establishes a narrower readiness
check. Confirm the response belongs to this application if another service could
already occupy the port. Leave the service running when requested and report its
name, ID, and how readiness was checked. If readiness times out, inspect state,
`uc explain NAME --json`, and bounded logs.

## An interactive session

```sh
uc start uc-example-shell --interactive --json
uc peek uc-example-shell
uc send uc-example-shell 'printf "input received\n"' --json
uc logs uc-example-shell --max-bytes 4096 --json
```

Before sending, establish which foreground program receives input. The same text
could be a shell command, a chat message, or an answer to a prompt. A successful
delivery does not show that the application consumed it.

`send` normally adds Enter after a nonempty body. Use `--no-newline` to suppress
that Enter, `--paste` for bracketed paste, or `--key enter` for a key-only request.
Use paste only when the foreground application understands it; plain stdin
readers receive its control sequences literally. Omit `--paste` for those readers.
For a UTF-8 task file, `uc send NAME --paste --json < /absolute/task.md` reads
the caller's stdin. Bound file size to the installation's request limits.
Do not interpolate untrusted file contents into shell source.

When a real terminal is available, `uc attach NAME` enters the terminal.
Default detach is Alt-U, release, then `d`. Detaching leaves the session running.

## A separate command in session context

```sh
uc exec uc-example-shell --dir-launch --env-launch -- /bin/sh -c 'printf "separate command\n"'
```

This deliberately uses saved launch directory and environment. Omitting those
flags requests current directory and captured root-shell exports; those require
available live context and shell integration. `exec` provides a finite result,
forwards validated stdout/stderr, and returns the child's representable exit code.
Missing/truncated output or integrity failures have their own CLI errors. Do not
rerun a mutating command merely to recover a lost result.

## Stop and clean up

```sh
uc stop uc-example-shell --graceful --json
uc inspect uc-example-shell --json
uc clean uc-example-shell --yes --json
```

Run cleanup only after confirming terminal state and preserving needed evidence.
Graceful stop can escalate after the configured stop timeout. Plain `stop` and
`restart` kill immediately by default. A stop timeout does not cancel an accepted
stop; reconcile it before escalating. Automatic restart intent can accompany an
exited-looking session, so inspect restart status as well.

Use explicit session names for automation. Avoid unscoped `stop --all` or `clean`;
bulk actions can include unrelated work and can partially succeed. Restart
intentionally creates a new process incarnation from saved configuration.
