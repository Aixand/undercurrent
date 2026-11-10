# Choose a finite operation and interpret its result

Use this reference to select requests and understand what successful responses
establish. It covers the current product contract `0.1.0-beta`. The authoritative
inventory is [ProductOperation::ALL](../../../crates/undercurrent-protocol/src/registry.rs), with request and result
shapes in [dto.rs](../../../crates/undercurrent-protocol/src/dto.rs); the
[registry fixture](../../../crates/undercurrent-protocol/tests/fixtures/registry_operations.json) pins all 34 names.
Use generated Rustdoc for every field and enum variant.

The protocol crate describes these operations but cannot execute them. Use an
already connected [Rust SDK client](../../../crates/undercurrent-sdk/README.md) or the
[Python SDK's supported subset](../../../python/README.md). Numeric DTO fields
such as page size, terminal geometry, and timeouts remain subject to daemon and
domain validation. A Rust `u32` field is not permission to request its maximum.

Jump to [session mutations](#sessions-and-process-effects), [queries and observations](#queries-projections-and-observations),
[work and profiles](#external-work-and-detection-profiles), or [diagnostics and recovery](#diagnostics-recovery-and-shutdown).

## Sessions and process effects

All operations in this table are classified mutating. Construct the appropriate
current `MutationContext` and read [retry rules](recovery.md) before resending.

| Operation and request intent | Successful result and boundary |
| --- | --- |
| `start_session`<br>Launch a named session with command, environment, terminal, execution policy, optional profile and attached dispatch. | `SessionMutationResultDto`: summary, changed flag, optional executable-resolution receipt. Normal success follows durable Running and owned process binding, not application readiness. |
| `stop_session`<br>Stop a stable session; optional force, graceful-only, and expected aggregate revision. | `StopSessionResultDto`: summary, already-terminal flag, optional exit code. Explicit control differs from request cancellation. |
| `restart_session`<br>Start a new process incarnation for the same session identity. | `SessionMutationResultDto`; do not equate the stable session with an unchanged process instance. |
| `rename_session`<br>Change the unique human-readable name. | Summary with the same stable session ID; clients should retain that ID. |
| `update_session_labels`<br>Atomically insert/replace `set` keys and delete `remove` keys. | Summary and changed flag. Other labels, private metadata, launch configuration, and process identity are preserved; identical patches are no-ops. |
| `remove_session`<br>Remove a terminal session with `Confirmed` or `Forced` presentation evidence. | Session ID and removal revision. Confirmation is not an authorization bypass or an implicit process stop. |
| `resize_terminal`<br>Request durable/live columns and rows. | Updated summary; runtime or persistence failures can require reconciliation. |
| `send_terminal_input`<br>Deliver an ordered item sequence, optionally with delivery ID and expected revision. | Delivery timestamp, duplicate flag, optional foreground revision. This proves terminal delivery, not application acceptance or task completion. |
| `close_standard_input`<br>Close the managed process's input stream. | Closure timestamp and `already_closed`; distinct from injecting an EOF byte or stopping the process. |
| `execute_in_session`<br>Execute a bounded one-shot command using a session's resolved directory and environment. | `ExecuteInSessionResultDto`: attempt certainty, optional exit code, stdout/stderr representations. [Output contracts](streams.md#command-output) govern integrity and truncation. |

### Launch defaults and executable identity

These are the DTO defaults. Client adapters can select different values; for
example, the [CLI defaults to native launch](../cli-commands.md#start-options).

| Start setting | DTO default |
| --- | --- |
| Executable launch | `Sealed` |
| Output interpretation | Terminal |
| Profile binding | Fixed; see [domain and client default scopes](../../../crates/undercurrent-domain/docs/internals/observations-and-profiles.md#base-profile-and-effective-profile-have-different-lifetimes). |
| Automatic restart | Never |
| Detection | Enabled |
| Automatic removal | Disabled |
| Stop signal | `Terminate` |
| Stop escalation | 5,000 ms before forced escalation is permitted |

`Native` launch runs from the original installation path; `Sealed`
uses verified immutable executable bytes. Attached dispatches require sealed
launch. A receipt records requested reference, resolved absolute program,
content digest, and receipt digest; native acceptance does not pin future bytes
in the same way as sealed execution.

`CommandSpecDto.arguments` is an ordered argument vector without a synthetic
`argv[0]`. The protocol does not implicitly interpret shell syntax.

A working
directory and environment file are references resolved by adapters. Environment
values and secret handles have [separate persistence rules](encoding.md#identity-environment-and-privacy).
Labels are query-visible; metadata is private and absent from compact lists.

Opaque capture is an optional negotiated capability; it preserves bytes without
terminal interpretation, so screen requests are not a substitute for raw reads.

### Restart scheduling

`RestartPolicyDto` distinguishes never, on-failure, and always. The latter two
carry maximum consecutive attempts, initial/maximum delay, multiplier in basis
points (10,000 means 1×), and uptime before the attempt counter resets.

Inspect
`restart_status` for idle, scheduled with attempt/time, not-scheduled with reason,
or exhausted. Reasons include policy disabled, successful exit, and explicit stop.
A scheduled time is an earliest time, not proof that a restart occurred.

### Revision preconditions

Stop and input `expected_revision` are optional aggregate-revision preconditions
checked under the session mutation lease **before** state/process effects or
input-delivery reservation. They require the session-revision-precondition
capability. A mismatch yields `revision_precondition_failed`; reread the session
and decide whether the action is still appropriate.

Changing this revision
changes the semantic fingerprint. It is separate from an attempt fencing token,
lifecycle revision, output offset, or event sequence.

### Input delivery

| Input rule | Contract |
| --- | --- |
| Item forms | Automatic profile-selected text, literal text without newline, bracketed paste, named keys, raw bytes, and mouse events. |
| Key variants | Navigation, function, control, and supported modified navigation/function keys. |
| Coordinates | Mouse coordinates are one-based; terminal screen cursor coordinates are zero-based. |
| Ordering | Send the full intended sequence together. Ordering is relative to peer sequences; it does not prove that an application acted on it. |
| Observation after delivery | `ForceActive` (default) versus `Preserve` controls best-effort observation after confirmed delivery. |
| Retry identity | Keep a delivery ID to reconcile deliberate retries. Changed content under the same identity is not a new delivery. |

### One-shot execution

`execute_in_session` defaults to the observed live directory and latest shell
exports. `dir_launch` and `env_launch` select saved launch settings independently
and participate in the fingerprint.

`output_inline_limit_bytes` applies across stdout and stderr. Optional
`deadline_millis` is an absolute Unix-epoch daemon-clock
deadline, unlike a relative SDK wait timeout. Neither timeout implies no effect.

### Attached start policy

`RuntimeDispatchBindingDto` binds caller-cell identity and caller request,
launch-profile, executable, environment-binding, and input digests. Its runtime
policy carries positive requested resource ceilings, output schema/digest and
classification, telemetry/retention references, sorted unique callback references,
and launch/environment expiry times. The daemon validates and retains this policy.
An absent policy remains decodable for old retained state but is rejected for
new attached dispatches.

`memory_bytes` and `cpu_millis` are required-present nullable members of a present
policy. Their omission is not equivalent to explicit `null`.

Requested wall time,
stdout, stderr, memory, and CPU limits are audit facts: these DTOs **do not prove
OS enforcement**.

`RuntimeOutputContractDto` deserialization rejects `Secret`
classification because the inline runtime-result path cannot carry it; expressing
an output-reference variant elsewhere does not enable an arbitrary content adapter.
See [dispatch-policy tests](../../../crates/undercurrent-protocol/tests/dispatch_binding.rs) and
[exact status lookup](recovery.md#read-an-attached-runtime-attempt).

## Queries, projections, and observations

| Operation and request intent | Result and interpretation |
| --- | --- |
| `list_sessions`<br>Filter, sort, choose projection, page size/cursor, optional total. | `SessionPageDto`: summaries, next cursor, snapshot revision, optional total. |
| `get_session_detail`<br>Choose private/detail sections for one stable ID. | `SessionDetailDto`: summary plus requested configuration, work, process, foreground evidence, health results; persistence flag and optional shell/restoration metadata. |
| `get_execution_context`<br>Resolve a private execution environment and working directory. | `ExecutionContextDto` contains values; never log it or expose it as list/detail data. |
| `read_terminal_output`<br>Read at a byte offset with a maximum length; [size the encoded page](streams.md#budget-the-encoded-response). | Data or explicit retention gap; continue from `to_offset`, not the source end. |
| `read_terminal_screen`<br>Obtain current rendered terminal state. | Snapshot with a client-scoped checkpoint and exact output offset. |
| `read_terminal_screen_diff`<br>Advance an issued client-scoped screen checkpoint. | ANSI diff or replacement snapshot, each with a new checkpoint. |
| `read_session_events`<br>Filter session events, choose last consumed sequence/limit, optionally tail and include event-time observation. | Event page, checkpoint, has-more flag, and optional explicit gap; [retention is bounded](streams.md#four-kinds-of-position). |

### Query cursors and projections

Session filters cover IDs, exact case-sensitive name, name substring, lifecycle,
execution, interaction, health, profile, labels, and active work. Sort defaults
to name ascending; other fields include lifecycle, execution, interaction,
health, update time, and restart count. Projection defaults to compact, with
label and operational alternatives. Preserve a query's filters/sort/projection
when following its opaque cursor; restart the query on cursor expiration.

Detail projection flags default false. Configuration detail reports environment
**names**, file reference, directory, launch settings, labels and metadata, not
resolved values. Process detail identifies an incarnation and its start time.
Shell environment status provides only capture identity/time. A restoration
directory is an observation retained for restoration, not a proof of current cwd.

### Interpret session observations

Keep these independent axes separate:

| Field family | Meaning and limitation |
| --- | --- |
| Lifecycle | Authoritative starting/running/stopping/stopped/exited/failed state; normal summary `Exited` may have no code. Exact attempt/cancellation snapshots require an exit code when exited. |
| Foreground execution | Best-effort active/quiescent/unknown observation. |
| Interaction | None/available/requested plus optional text/options/secret/navigation modality and profile semantic. Available input does not imply completed work. |
| Runtime constraint | Clear/present, independently observed. |
| Observation basis | Confidence basis points, source, affirmative/inference/unavailable disposition, commit time, and optional evidence-observed time. Fresh commit does not make old evidence fresh. |
| Health | Healthy/unhealthy/unknown/no-check, observation time, and configured check count. Rules may use TCP, HTTP, output patterns, red cells, or a command. |
| WorkContext | External owner declarations and their aggregate counts/status, independent of process or foreground truth. |
| Profile context | Base/effective origin, fixed/adaptive binding, epoch, matching evidence, suppressed fallback candidates, unclassified reason, optional current directory and server-role evidence. |

An effective server role can be supported by a profile, foreground TCP listener,
managed root listener, or managed-tree listener (including background children).
Those evidence origins do not all assert foreground membership. Use the detailed
basis instead of interpreting a single summary field as an application-level result.

## External work and detection profiles

| Operation and request intent | Result |
| --- | --- |
| `upsert_work_item`<br>Mutating insert/replace of one owner/item-ID declaration. | Updated session summary and changed flag. |
| `remove_work_item`<br>Mutating removal by session, owner, and item ID. | Updated session summary and changed flag. |
| `list_profiles`<br>Bounded cursor page. | Profile summaries: name, source, description, disabled flag. |
| `get_profile`<br>Inspect one profile. | Schema version, archetype, input delivery policy, matching patterns, required environment names, foreground/background rule counts. |
| `match_profile`<br>Match executable, arguments, and environment **names**. | Selected profile or none, plus candidates. |
| `lint_profiles`<br>Validate configured profiles, optionally one name. | Validity and safe field/severity diagnostics. |
| `test_profile`<br>Evaluate supplied screen text, last line, and cursor-at-end sample. | Rule indices, match/exclusion evidence, typed dispositions, winning foreground claims and confidence; no retained screen sample. |

### Work declarations and profile delivery

Work declarations include relationship (depends-on, delegated, spawned, related),
status, title/description, provenance, creation/update/expiry times, metadata,
and session/label/external targets. The caller owns their meaning; completion of
a declaration is not derived from process exit. Status includes delivery failure
and expiration as well as running/waiting/blocked/completed/failed/cancelled.

Profile delivery is immediate or paced trailing submit. The paced policy flushes
the body, waits its declared delay, then sends one trailing Enter. Text mode is
raw or paste. Inspect the selected profile rather than adding hidden newlines
or assuming every process accepts the same prompt encoding.

## Diagnostics, recovery, and shutdown

| Operation and request intent | Result and boundary |
| --- | --- |
| `get_diagnostics`<br>Select bounded sections/page/cursor. | Payload-safe rows and capture time. |
| `create_diagnostic_bundle`<br>Export a bounded diagnostic page with optional explicitly authorized terminal content. | Diagnostics, optional terminal page, and redaction flag. Registry classifies this finite export as nonmutating; it creates no runtime attempt metadata. |
| `get_daemon_status`<br>Read daemon identity and pressure. | Server epoch, start time, accepting-connections flag, connection/stream/session counts. |
| `shutdown_daemon`<br>Mutating request with drain timeout. | `ShutdownAcceptedDto` gives drain deadline; acceptance is not daemon exit or proof all runtime effects completed. |
| `repair_operation_attempt`<br>Mutating evidence-backed repair with its own direct contract. | Terminal certainty plus post-commit unresolved-readiness projection; see [repair](recovery.md#repair-an-unknown-attempt). |
| `get_runtime_attempt_status`<br>Exact attached attempt ID and caller request digest. | Binding/fence and unknown/absent/applied/compensated snapshot; read-only. |
| `cancel_runtime_attempt`<br>Mutating, independently authorized exact control. | Target/control bindings and durable cancellation state; see [cancellation](cancellation.md). |
| `get_runtime_cancellation_status`<br>Exact control, command digest, predecessor/session/provider and current caller context. | Same retained state without process IO or redrive. |
| `get_agent_event`<br>Bounded poll for the next unsettled exact-attempt event. | None with next sequence or one event; see [agent exchange](agent-events.md). |
| `reply_agent_event`<br>Mutating reply to exact event/call/process binding. | Durable reply digest and duplicate flag. |

### Diagnostic bounds and privacy

Safe diagnostic sections are connections, streams, slow consumers, output
producers, supervisor queues, detection evidence, persistence, reconciliation,
and configuration. `CreateDiagnosticBundleRequest::validate()` requires nonempty
unique sections and positive page size. Optional terminal content must request
1–16,384 bytes for one session; export and terminal access still need authorization.
Scalar diagnostic values are boolean, signed/unsigned integer, or text. Their
DTO representation alone does not sanitize arbitrary text supplied by a producer.

For failures use [typed errors and recovery](recovery.md), not success-only
branching. For event subscriptions, watched pages, and terminal offsets use
[the stream contract](streams.md). The [maintainer guide](../../../crates/undercurrent-protocol/docs/README.md)
traces ownership and durable commits behind these results.
