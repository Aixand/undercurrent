# Control sessions and deliver input

Use this reference when integrating Python SDK 0.1.0-beta into an asyncio application.
For a complete runnable use of `Client`, start with the
[input round trip](../../../python/examples/input_roundtrip.py) and its
[setup and cleanup](quickstart.md). Use the sections below to look up
[connection options](#connect-and-share-a-client), [launch options](#start-and-identify-a-process),
[input](#deliver-text-or-keys), [stop](#stop-a-session), or [returned values](#understand-returned-values).

## Connect and share a client

`Client(home=None, bridge=None, request_timeout=30.0, request_bytes=33552594)` is lazy: constructing one
resolves the home but starts no bridge. `await client.connect()`, entering `async with Client()`, or the first
operation starts its bridge and connects. Prefer the context manager so the
bridge is closed on exit.

### Home

`home`: Explicit nonempty value, then `UNDERCURRENT_HOME`, then `~/.aixand/undercurrent`; Python expands `~` and resolves an absolute path at construction. `client.home` returns it.

### Bridge executable

`bridge`: Explicit nonempty executable, then `UNDERCURRENT_PYTHON_BRIDGE`, then `uc-python-bridge`; resolved with `shutil.which` at connection. Use an absolute path or a name on `PATH`. No shell command or argument string is accepted.

### Request timeout

`request_timeout`: Finite positive int/float, excluding booleans, at most 300 seconds. Bounds the ready handshake and is passed to each finite request or subscription open. See [deadline boundaries](recovery.md#deadlines-and-cancellation).

### Request size

`request_bytes`: Positive integer, excluding booleans, up to `2**32 - 1 - 1838`.
Defaults to 33,552,604 bytes for the complete product JSON request. The bridge
sizes its SDK profile and private line buffer accordingly. The client does not
read daemon configuration: configure the daemon independently and restart it
when changing its limits. Its input allowance and negotiated transport limits
can be smaller than the client offer.

For example, use `Client(request_bytes=67107036)` with the coordinated
[10 MiB input configuration](../../guides/configuration/README.md#configure-terminal-input-size).
This is a byte ceiling, not a timeout, retry budget or promise of that many text
characters. See the [decision record](../../architecture/decisions/0004-input-size-limits.md).

### Options for a fresh client

`connection_options`: A new dict of `home`, `bridge`, `request_timeout`, and `request_bytes`, usable as `Client(**options)` to create a fresh connection. After a successful executable lookup it includes the resolved bridge path. It contains no session or stream checkpoints.

### Share and close

Share one client among tasks on **one event loop**. It has a connection lock,
serialized writes, one reader task, correlation IDs, and up to 16 pending calls.

Concurrent coroutine submission does not establish process-effect order. Await
dependent mutations sequentially; use revision preconditions for state-sensitive
operations.

Cross-thread, cross-loop, and post-fork reuse have no supported contract.
Each separately constructed client launches its own bridge.

`await client.close()` permanently closes that instance; it closes bridge stdin,
waits up to two seconds, kills the bridge if necessary, and cancels its reader.
It does not stop the daemon or sessions.

Close streams and finish or cancel your
tasks before closing the shared client. A closed or failed bridge requires a
fresh `Client`, as described in [recovery](recovery.md).

## Start and identify a process

`await client.start(command, ...)` accepts a nonempty sequence of argument
strings, such as `["/bin/sh", "-c", "printf hello"]`. A single command string or
bytes is rejected. There is no implicit shell interpolation, piping, or globbing;
an explicitly launched shell owns those semantics.

### Session name: `name`

Unique human-facing name; omitted or empty becomes `python-<uuid>`. Supply and save a unique name before starting if response-loss reconciliation matters.

### Working directory: `cwd`

Omitted or empty uses the caller's working directory; expanded and made absolute before sending. The bridge supplies the standalone environment binding for it.

### Process environment: `env`

Mapping of explicit environment variables; defaults to empty. The SDK does not copy all of Python's environment into the launch request or supply an environment file. Runtime launch policy resolves the process environment; pass required values explicitly.

### Detection profile: `profile`

Explicit detection profile, or daemon selection when absent. Missing selected profiles are rejected.

### Binding mode (`profile_binding`)

Defaults to `"fixed"` to preserve existing caller behavior. Use
`profile_binding="foreground_adaptive"` to follow detected foreground applications,
including applications launched through wrappers. Both modes work with an omitted
or explicit base profile; adaptive selection changes runtime effective state, not
the saved base profile. Invalid modes raise `ValueError` before bridge IO.

### Terminal size: `columns`, `rows`

Initial terminal size, defaults 120 by 40; wire/domain validation applies. Python exposes no resize method.

### Enable detection: `detection`

`True` by default; controls runtime observation detection. Disabling it does not stop lifecycle supervision.

### Query labels: `labels`

Query-visible string mapping, empty by default. Useful for identifying one caller-owned run.

### Start request identity: `idempotency_key`

Supplied key or fresh UUID goes in mutation metadata. Ordinary Python starts do not carry a pinned dispatch/attempt and have no general replay guarantee from this key alone. See [start reconciliation](recovery.md#reconcile-a-mutation).

### Launch behavior and result

Python requests **native** launch mode, terminal output capture, and restart mode
`never`. It does not expose sealed launch, attached dispatch, health-rule
configuration, restart policies, environment files, private metadata, or opaque
output capture.

Start success returns a `Session` summary after the start
service's activation work; it does not prove prompt readiness. The process can
exit immediately after that snapshot. [The shared creation sequence](../../architecture/flows.md#start-a-session)
shows the separate durable and process-effect boundaries.

### Find an existing session

All methods targeting one session accept its `Session` snapshot or stable ID.
They extract `Session.id` or stringify the supplied value; they do **not** resolve
names. `await client.list(name="exact-name")` is the explicit discovery step.
Save the returned ID for subsequent mutations, since names can be changed or
reused by other clients.

### List and inspect

`list(labels=None, ids=None, name=None)` collects operational pages of 100 into
one Python list. IDs restrict membership, labels select matches, and `name` is
exact and case-sensitive. The protocol's default order is name ascending.

This helper hides cursors and page metadata and is not a streaming iterator: memory
grows with the full result. A cursor failure aborts the call without returning a
partial list. Restart a read query explicitly if its snapshot expired.

`inspect(session)` reads current detail but returns only its `summary` as a
`Session`; it does not expose full configuration or evidence projections.

### Rename a session

`await client.rename(session, "new-name")` changes a session's human-facing name
and returns its summary. Its stable ID, process, labels, and execution settings
remain the same. If the response is lost, inspect the saved session ID to check
its current name before retrying.

## Deliver text or keys

`send_input(session, text, submit=True, delivery_id=None,
expected_revision=None, preserve_observation=False, paste=None)` writes one
ordered input sequence: text, then Enter if requested. `text` must be a string;
`paste` must be `None` or a bool. The whole sequence is ordered relative to peer
deliveries, but a failed terminal write is not rolled back.

### Follow the profile: `paste=None`

Sends `auto_text`; the daemon uses the effective profile's
`input_delivery.text_mode`. The domain default is raw text.

### Use bracketed paste: `paste=True`

Sends explicit bracketed paste. Use only with a receiving application that understands it.

### Send literal text: `paste=False`

Sends literal text without paste wrappers.

### Omit Enter: `submit=False`

Omits Enter; text may remain in an unsubmitted editor.

### Mark foreground activity: `preserve_observation=False`

Default `force_active`: after delivery, best-effort foreground state becomes active/interaction none, retaining the constraint.

### Keep the observation: `preserve_observation=True`

Preserves foreground observation for direct human-style interaction. It supplies no new readiness barrier.

### Guard the input revision: `expected_revision`

Optional exact aggregate revision checked under the daemon's session mutation lease **before** reservation or terminal IO. A mismatch is `revision_precondition_failed`; inspect and reconsider the action rather than replacing the revision blindly.

### Check the effective profile

A runtime home's editable profile can differ from the shipped defaults.
When diagnosing input delivery to a process, check its effective profile's
`input_delivery.text_mode`. Input uses the effective profile at delivery time.

For example, the shipped [Codex profile](../../../crates/undercurrent-daemon/profiles/codex.yaml)
selects paste mode. A locally edited copy can change that setting.

Profile pacing can delay trailing Enter while preserving the single delivery's
ordering. The SDK cannot infer whether a target application accepted a prompt.
For profile ownership and the
write boundary see [input delivery internals](../../../python/docs/internals/conversion.md#deliver-input).

### Send one key

`send_key(session, key, delivery_id=None)` sends one key, using the same receipt
and default observation policy. Use a simple key string or a structured `KeyDto` value:

| Key form | Python value |
| --- | --- |
| Simple | `"enter"`, `"escape"`, `"tab"`, or `"up"` |
| Control | `{"key": "control", "character": "c"}` |
| Function | `{"key": "function", "number": 5}` |
| Modified | `{"key": "modified", "base": {"key": "up"}, "modifiers": {"shift": True}}` |

The exact vocabulary and validation belong to
[`KeyDto`](../../../crates/undercurrent-protocol/src/dto.rs) and the
[terminal encoder](../../../crates/undercurrent-protocol/src/terminal.rs).
`send_key` has no Python revision-guard or preserve-observation option.
Control-C is terminal input, not a guarantee of process termination.

### Preserve the input identity

Choose and persist a `delivery_id` before sending when recovery matters. Omitting
it generates a new UUID for that invocation.

A deliberate same-ID retry must
retain the same session, input items (including paste/Enter selection), and
observation policy. Follow [the recovery procedure](recovery.md#reconcile-a-mutation)
to check preconditions and resolve ambiguity before retrying.

The daemon retains delivery records in SQLite through restart until the owning
session is deleted. An unresolved delivery remains indeterminate; persistence
does not authorize another write.

## Stop a session

`stop(session, force=False, expected_revision=None)` requests termination and
returns the resulting `Session`.

Default stop permits graceful termination and
the runtime's escalation policy; it is not the protocol's `graceful_only` mode.
`force=True` requests immediate forced termination.

Revision checking occurs
before state changes or signaling. Each invocation generates a new mutation key;
Python exposes no caller key for stop.

The daemon treats stopping an already-terminal session as idempotent, but Python
discards the protocol result's `already_terminal` and separate `exit_code`
fields. Inspect the returned lifecycle and, when present, exit detail in `raw`.

A failed stop can leave a durable `stopping` state requiring reconciliation.
Do not equate timeout with an unstopped process or retry with `force=True`
automatically. Stop does not delete the record.

## Understand returned values

These wrappers are defined in [`models.py`](../../../python/src/undercurrent/models.py).
They are frozen dataclasses, but `Session.raw` is still a mutable dictionary;
treat it as read-only. No wrapper owns the underlying process or refreshes itself.

### Session snapshot: `Session`

Snapshot exposing `id`, `name`, aggregate `revision`, and the five [state axes](state.md). `raw` retains the summary, including `lifecycle_revision`, labels, timestamps, profile context, work summary, restart status, and lifecycle exit code when known. Missing keys raise ordinary Python lookup errors; this is not a schema validator.

### Input receipt: `InputReceipt`

`session_id`, `delivered_at_millis`, `delivery_id`, `duplicate`, optional `foreground_revision`. Confirms terminal delivery or a retained delivered receipt, not application acceptance. A duplicate normally has no new foreground revision; even a fresh delivery can return `None` if no foreground commit was needed or observation was preserved.

### Output page: `OutputPage`

Bytes plus `from_offset`, `next_offset`, `has_more`; `text(encoding="utf-8", errors="replace")` decodes that page only. See [output handling](streams.md#read-retained-output).

### Watch update: `SessionUpdate`

A `session` snapshot and `resynchronized` flag. Initial/replacement snapshots and gap recovery set it to true; ordinary deltas default false.

Continue with [state waiting](state.md), [stream lifetimes](streams.md), and
[typed errors and recovery](recovery.md). Return to the [Python entry point](../../../python/README.md).

## Contract owners

The supported imports are defined by
[`__init__.py`](../../../python/src/undercurrent/__init__.py); behavior is implemented in
[`client.py`](../../../python/src/undercurrent/client.py). The authoritative product data
contracts are [`dto.rs`](../../../crates/undercurrent-protocol/src/dto.rs), with
operation names in the [protocol registry](../../../crates/undercurrent-protocol/src/registry.rs).
Private `_request` and `_bridge` are implementation details, not a generic
supported Python product API.

## Session labels and private metadata

```python
session = await client.start(
    ["/bin/sh"], labels={"app": "project-x", "role": "reviewer"},
    metadata={"inputs.path": "/work/project-x/inputs.json"},
)
related = await client.list(labels={"app": "project-x"})
private = await client.metadata(session)
session = await client.update_labels(session, labels={"aixand.my_terminal": "true"})
session = await client.update_labels(session, remove=["aixand.my_terminal"])
```

Both maps contain strings and are validated before bridge IO. Each accepts 64
entries; keys accept 64 UTF-8 bytes, label values 256 bytes, and metadata values
4096 bytes. Keys must be nonempty, and control characters are rejected. Encode
structured values as JSON strings and retain larger artifacts separately.
Labels support list filtering. Metadata is omitted from summaries and ordinary
`inspect`; `metadata(session)` explicitly requests configuration detail.

`update_labels` atomically changes only the specified keys and preserves all
other labels and private metadata. Keys cannot be set and removed in the same
request. A resulting map must still fit the 64-label limit. The session is not
restarted; the returned summary contains the updated labels.
