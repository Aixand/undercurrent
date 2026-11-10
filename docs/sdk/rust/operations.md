# Perform product operations and reconcile their outcomes

Use `ProductClient::request(ProductRequest, timeout)` for finite operations in
Rust SDK 0.1.0-beta. Start with a connected client from the
[connection tutorial](connect-and-query.md). This guide explains caller decisions;
exact fields and variants belong to the
[protocol DTOs](../../../crates/undercurrent-protocol/src/dto.rs),
[operation registry](../../../crates/undercurrent-protocol/src/registry.rs), and
[current-contract validation](../../../crates/undercurrent-protocol/src/compatibility.rs).

Start with the [disposable-session example](#create-and-clean-up-a-disposable-session),
or jump to the [operation map](#choose-the-right-operation),
[mutation setup](#prepare-a-mutation), or [reconciliation](#reconcile-a-mutation).

## Create and clean up a disposable session

The complete [create_and_stop.rs](../../../crates/undercurrent-sdk/examples/create_and_stop.rs) example creates
one new session, immediately stops it, then removes its terminal catalog record.
It uses a native launch, default no-restart policy, current mutation metadata,
and explicit noninteractive removal confirmation. It is intended for an
**isolated test daemon home** and a disposable program, not an existing session.
It never retries failed operations.

### 1. Run once with a unique name

From the repository root, with a test daemon already running, replace the first
two arguments with its endpoint and a new unique name. This Unix example runs
`sleep`, then stops it; it is not a workload-completion demonstration:

```sh
cargo run --locked -p aixand-undercurrent --example create_and_stop -- /absolute/test-home/run/control.sock sdk-disposable-001 /bin/sleep 60
```

On Windows, supply a disposable native executable and its argument vector for
that platform. The Unix `/bin/sleep` invocation is not portable. No shell expansion
or implicit synthetic `argv[0]` is added by `CommandSpecDto`.

### 2. Verify removal

Expected output is a `creating disposable session named ...` line, a `created
<ID>; stopping it now` line, and a final `removed <ID>` line. These are expected
shapes, not recorded execution. The example closes the client even on errors.
Verify success with a new filtered list query for the chosen name: the record
should be absent after the final remove result.

### 3. Reconcile cleanup after an error

If any step fails, preserve the printed name/ID and reconcile before running the
example again. If start's response was lost, list by exact name and inspect the
candidate's configuration and identity; do not assume a same-name session is
owned by this example. If stop/remove failed after a known start, inspect that
exact ID. Finish cleanup with a separately chosen `StopSession` and, only after
terminal state and explicit deletion approval, `RemoveSession`. A disconnected
client does not clean up daemon-owned processes or records. An absent session
after remove response loss is useful outcome evidence, not authorization to
repeat some other mutation.

The shared [start sequence](../../architecture/flows.md#start-a-session) shows
why a lost response can follow durable commits and process effects. Attached
starts also have durable attempt admission and settlement; inconclusive evidence
can leave an exact process live. Use [SDK admission](../../../crates/undercurrent-sdk/docs/internals/client-runtime.md#session-creation)
when investigating whether the request entered the writer.

## Choose the right operation

The SDK forwards the protocol's finite operation registry through one typed
request path. It does not implement a separate product inventory or launch
processes itself. Use this task map to find the relevant family; the registry owns the complete inventory.

| Task | Protocol operations | Caller responsibility |
| --- | --- | --- |
| Manage session lifecycle and identity | `StartSession`, `StopSession`, `RestartSession`, `RenameSession`, `RemoveSession` | Supply current mutation metadata, retain stable IDs, reconcile effects, collect removal confirmation |
| Send terminal input | `ResizeTerminal`, `SendTerminalInput`, `CloseStandardInput` | Preserve input order and delivery identity; distinguish delivery from application completion |
| Find and inspect sessions | `ListSessions`, `GetSessionDetail` | Select filters/projections and handle pagination and stale cursors |
| Execute in an existing context | `GetExecutionContext`, `ExecuteInSession` | Treat returned environment/output as private; follow attempt, deadline, and output-storage contracts |
| Read output and events | `ReadTerminalOutput`, `ReadTerminalScreen`, `ReadTerminalScreenDiff`, `ReadSessionEvents` | Preserve byte offsets, screen checkpoints, and durable event cursors separately; handle gaps |
| Declare work | `UpsertWorkItem`, `RemoveWorkItem` | Supply owner/item identity and current mutation metadata; work declarations do not execute work |
| Investigate detection profiles | `ListProfiles`, `GetProfile`, `MatchProfile`, `LintProfiles`, `TestProfile` | Supply bounded queries/samples; detection observations are not command results |
| Operate and diagnose the daemon | `GetDiagnostics`, `CreateDiagnosticBundle`, `GetDaemonStatus`, `ShutdownDaemon` | Page diagnostics, opt into authorized content export, distinguish shutdown acceptance from exit |
| Reconcile exact attempts and controls | `RepairOperationAttempt`, `GetRuntimeAttemptStatus`, `CancelRuntimeAttempt`, `GetRuntimeCancellationStatus` | Use exact caller/provider/attempt/fence bindings; avoid mutation redrive on response loss |
| Service an attached agent runtime | `GetAgentEvent`, `ReplyAgentEvent` | Preserve exact binding, event sequence, event identity, and digests; respect expiry and authorization |

Dedicated `ProductClient` conveniences return the specific DTO for diagnostic
bundle creation, runtime cancellation/status, and agent event poll/reply. Other
operations use `request`. All go through the same codec and correlation checks.
A successful response for a different operation becomes `OperationMismatch`;
an operation-less error is accepted only for a correlated cancellation result.

## Query and inspect without changing state

Use a `ListSessionsRequest` with an explicit nonzero bounded page size, desired
filter, sort, and projection. The [query example](../../../crates/undercurrent-sdk/examples/query_watch.rs)
shows a complete initial page. For subsequent pages, carry `next_cursor` into the
same query. A cursor is opaque; do not edit it or substitute it for a revision.
On `CursorExpired`, discard that traversal and restart from a fresh page rather
than joining unrelated snapshots.

Use `GetSessionDetail` with only the needed `SessionDetailProjectionDto` sections.
The default detail projection requests none of the optional large/private
sections. Retain the returned stable ID when targeting a later operation; a
human-facing name can be renamed. Summary revision is an aggregate revision,
while lifecycle revision changes on lifecycle transitions. Foreground execution,
interaction, constraints, and health are separate observations. “Ready” or an
exit code alone does not prove a higher-level task succeeded.

Read output with a bounded `max_bytes` and advance using returned offsets.
Budget for the [entire encoded response](../../reference/protocol/streams.md#budget-the-encoded-response),
not just raw bytes; 32768 is suitable for standard settings, with smaller pages
needed for lower custom limits. The
[terminal guide](streams-and-recovery.md#observe-terminal-output) covers output
pages, binary streams, and retention discontinuities. Read events using their
global durable journal sequence, not the SDK stream sequence. Request observation
payloads only when needed; they may be absent from ordinary event reads.

## Prepare a mutation

For requests with a mutation context, construct
`MutationContext::current(ProductOperation::...)`. It sets the operation-specific
0.1.0-beta descriptor. `RepairOperationAttempt` instead owns a direct contract descriptor
and does not accept caller-supplied attempt fences or effect claims. `MutationContext::current` does **not** generate
an idempotency key, durable attempt, fence, or canonical payload digest. Attached
runtime integrations must supply those according to their own
[attempt contract](../../../crates/undercurrent-protocol/src/contract.rs) and
[canonicalization helpers](../../../crates/undercurrent-protocol/src/canonical.rs).

For input and stop, set `expected_revision` when your decision depends on a
particular aggregate revision. The SDK requires the negotiated session-revision
capability; the daemon checks the revision while holding mutation ownership
before the guarded effect. On `RevisionPreconditionFailed`, fetch current detail
and reconsider the action. Do not automatically drop the guard or merely replace
it with the latest number.

### Preserve terminal delivery identity

`SendTerminalInput` carries an ordered list of text/key/mouse inputs, an optional
`delivery_id`, and an observation policy. Preserve a delivery ID for the same
semantic delivery; changing IDs to bypass uncertainty risks duplicate input.
The result's `duplicate` field reports delivery deduplication. “Delivered” does
not mean the terminal application accepted the command or produced its intended
result. `CloseStandardInput` closes stdin for the selected session and has its
own `already_closed` result. Neither is subscription cancellation.

### Inspect one-shot execution results

For a one-shot `ExecuteInSession`, select directory/environment context explicitly,
retain attempt metadata, and use the request's absolute deadline if required by
the workflow. The result contains effect certainty, optional exit code, and
stdout/stderr representations: inline bytes, immutable content reference, or
stream metadata. The SDK returns those DTOs; it does not fetch content-reference
URIs, consume referenced output automatically, or validate application success
from an exit code.

## Reconcile a mutation

Keep the original request identity and semantic input until the result is known.
Use the shared [identity table](../recovery.md#keep-identities-and-evidence-separate)
to distinguish transport correlation, bounded request deduplication, and durable
attempt or delivery identities.

Follow the shared [mutation reconciliation procedure and response-loss diagram](../recovery.md#reconcile-the-mutation).
It owns cache scope, pending/conflicting requests, effect evidence, and the
operation-specific decisions after a missing response. A new client ID or expired
cache retention can remove deduplication protection; the SDK never replays a finite
request during reconnect.

For Rust callers, inspect `ProductClientError::Server(error)` for finite effect
certainty and keep optional disposition as `None` when absent. Broad categories
such as `Overloaded`, `Timeout`, `Cancelled`, and `Unavailable` cannot establish
that effects are absent. `retryable` and retry timing are server guidance, subject
to the operation contract. See [error boundaries](limits-and-diagnostics.md#error-boundaries).

For attached attempts, `GetRuntimeAttemptStatus` reads the exact caller/provider
binding; `RepairOperationAttempt` is an explicit evidence-backed mutation for
supported Unknown attempts, not a general “try again” operation. For an uncertain
`CancelRuntimeAttempt`, retain the exact control binding and call
`get_runtime_cancellation_status`. It reads durable status without process IO or
redrive. Graceful/force modes and `TargetOnly` propagation are owned by the
[runtime cancellation DTO contract](../../../crates/undercurrent-protocol/src/runtime_cancellation.rs).
A stronger force action is a separately authorized command, not an SDK timeout
fallback.

`GetAgentEvent` and `AgentEvents` subscriptions target the full tenant, actor,
authority, provider generation, session, attempt, revision, recovery epoch, fence,
and request digest. `ReplyAgentEvent` acknowledges one exact retained event and
canonical reply. Stale or expired bindings must be refreshed through the owning
integration; do not poll or reply against a different attempt just to make progress.
The [agent DTOs](../../../crates/undercurrent-protocol/src/agent.rs) own that contract.

## Cancel a wait or stop the daemon deliberately

Start a finite request with `request_cancellable`, call `cancel` if desired, and
still call `wait(timeout)`. A normal result can win the cancellation race.
Successful `cancel()` only means a cancellation command entered the local writer.
Timeout and dropping the handle perform best-effort cancellation; neither proves
rollback or process termination. The [cancellation test](../../../crates/undercurrent-sdk/tests/product_client.rs)
checks both a cancellation terminal and a winning normal result.

`ShutdownDaemon` is an explicit product mutation whose success carries a drain
deadline. It means shutdown was accepted, not that the daemon has exited.
`ProductClient::close` only releases the client side. Diagnose daemon pressure with
`GetDiagnostics` or `create_diagnostic_bundle`: the latter includes terminal
content only when explicitly requested for one session and separately authorized.
The SDK does not write a bundle file; the caller owns storage and sharing.

[Streams and recovery](streams-and-recovery.md) · [Errors and diagnostics](limits-and-diagnostics.md) · [SDK entry point](../../../crates/undercurrent-sdk/README.md)
