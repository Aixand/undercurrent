# Run and inspect your first session

Start a command, read its output, and clean up its session. This guide assumes
`uc` is installed and available in your terminal.

Run these commands from your project directory in a Linux or macOS shell.
For Windows, use the [PowerShell tutorial](first-session-windows.md).
Choose an unused session name; this example uses `hello`.

## 1. Start a command

```sh
uc start hello -- /bin/sh -c 'printf "hello from Undercurrent\n"'
```

`uc` creates a named session and starts the local daemon automatically when
needed. The response includes the session ID. The command runs in the managed
session, so you can inspect its result after it finishes.

## 2. Wait for it to finish

```sh
uc wait hello --lifecycle exited --timeout 10s
```

This returns when the command has exited. If it times out, inspect the session
with the commands below; a timeout does not stop the command.

## 3. Read the result

```sh
uc inspect hello --json
uc logs hello
```

The inspection should show exit code **0**, and the logs should include:

```text
hello from Undercurrent
```

The session and its output remain available until you clean them up.

## 4. Clean up

```sh
uc clean hello --yes
```

This removes the finished `hello` session and its retained output. Other
sessions and the daemon keep running.

## If a step fails

| Problem | Next step |
| --- | --- |
| The name `hello` is already in use | Choose another name and use it throughout the walkthrough. |
| Start reports an error or loses its response | Run `uc inspect hello` before starting it again; the session may already exist. |
| Wait times out or the command fails | Run `uc inspect hello --json` and `uc logs hello` to check its state and output. |
| Cleanup reports an active session | Run `uc stop hello`, then retry cleanup after it succeeds. |

For startup or configuration errors, see [troubleshooting](../guides/troubleshooting.md).

Next, [open an interactive shell](../guides/cli/interactive.md),
[control more sessions](../guides/cli/sessions.md), or
[automate with an SDK](../sdk/README.md).
