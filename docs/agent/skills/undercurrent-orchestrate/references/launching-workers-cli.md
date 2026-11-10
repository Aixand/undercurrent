# Launch workers through CLI

Use this only in selected CLI mode, after [preparing the task](launching-workers.md)
and checking [CLI control](uc-control.md). These POSIX examples use the saved
`uc_project`, `uc_run_dir`, `uc_run_id`, and `uc_worker_name` values from preparation;
on resume restore them from the manifest. Use native quoting on Windows. The
examples assume the selected agent is Codex; check its installed local help.

## Finite worker

After checking `codex exec --help`, record `launching` and submit:

```sh
uc start "$uc_worker_name" --cwd "$uc_project" \
  --label "uc-run=$uc_run_id" --restart never --json -- \
  codex exec --sandbox read-only --color never \
  --output-last-message "$uc_run_dir/architecture-result.md" \
  "Read the task in $uc_run_dir/architecture-task.md and complete it."
```

This is a review example. Preserve the user's chosen model and permissions when
adapting argv. Record `session.id` and the start receipt; do not add `--rm`.
The agent's `--output-last-message` exports the final message to the fresh path.

Start other independent workers before observing each with:

```sh
uc wait "$uc_worker_name" --lifecycle exited --lifecycle failed --lifecycle stopped --timeout 30s --json
uc inspect "$uc_worker_name" --json
uc logs "$uc_worker_name" --max-bytes 32768 --json
```

Read the final result file using ordinary file tools. Check its task identity,
substance, and the available exit code; wait success is not task success.

## Interactive worker

Record a new worker name and `interactive` mode, then launch:

```sh
uc start "$uc_worker_name" --cwd "$uc_project" \
  --label "uc-run=$uc_run_id" --restart never --json -- codex
uc peek "$uc_worker_name"
uc inspect "$uc_worker_name" --json
```

Wait for the task composer, not an approval dialog. Prepare the turn's task file,
fresh result path, and a unique delivery ID. Save that ID as `uc_delivery_id` and
in the manifest with the exact payload before sending:

```sh
uc send "$uc_worker_name" --paste --delivery-id "$uc_delivery_id" --json < "$uc_run_dir/architecture-task.md"
```

Use paste for a composer that understands bracketed paste; omit `--paste` for a
plain stdin reader, which would receive the control sequences literally. This
normally adds Enter after the body; use `--no-newline` if submission is not
intended. Save the receipt and any foreground revision. CLI waits have no
revision floor; verify a new task acknowledgment and result. An uncertain receipt
requires reconciliation before any resend, even with the same delivery ID.

For run-owned workers being stopped, use `uc stop NAME --graceful --json`, which
can escalate after the configured timeout, then inspect terminal state. Preserve
results and output before explicitly removing terminal records.
