# Look up the private session-host protocol

This reference helps maintainers change both ends of UCHS together. The
authoritative frame, operation, result, and failure registries are in
[process_host_protocol.rs](../../src/process_host_protocol.rs). They are private
daemon/session-host contracts, not supported Rust SDK or public client-wire
surfaces. Package, UCHS frame marker, receipt journal, and execution journal
all identify release `0.1.0-beta`. Both ends require the exact current identity.

## Frame envelope

Every frame has a 20-byte header followed by exactly the declared body length.
The codec validates magic, version, kind, checked length arithmetic, the configured
maximum complete-frame size, and exact length. Stream readers reject oversized
bodies before allocating them. JSON structs reject unknown fields. Required current fields must be present. Optional fields carry current optional
values; there is no historical decoding or cross-version negotiation.

| Header field | Size | Representation |
| --- | --- | --- |
| Magic | 4 bytes | ASCII `UCHS` |
| Release marker length | 1 byte | `10` |
| Frame kind | 1 byte | Registry below |
| Body length | 4 bytes | Unsigned big-endian, excludes header |
| Release | 10 bytes | ASCII `0.1.0-beta` |

| Kind/code | Body | Purpose |
| --- | --- | --- |
| Request / 1 | JSON `HostRequest` | Request ID and tagged operation |
| Response / 2 | JSON `HostResponse` | Same request ID and success value or typed failure |
| Exit / 3 | JSON `HostExitEvent` | Exact session/process, output checkpoint, outcome, observation time |
| Output / 4 | Binary | Identifiers, start offset, raw terminal bytes |
| Input / 5 | Binary | Request ID, identifiers, flags, optional pacing/attempt metadata, raw input bytes |
| Cooperative / 6 | JSON `HostCooperativeEvent` | Exact-process ready/running state and time, without command payload |

### Binary body layout

Fields occur in the listed order. Integers are big-endian; each identifier is
a `u16` byte length followed by that many UTF-8 bytes.

| Body | Ordered fields |
| --- | --- |
| Output | Session ID, process ID, `u64` start offset, remaining raw bytes |
| Input | `u64` request ID, session ID, process ID, one-byte tag, optional pacing, optional attempt, remaining raw bytes |
| Pacing | `u32` byte offset, `u64` delay in whole milliseconds |
| Attempt | `u16` JSON byte length, then JSON attempt context |

| Input tag | Pacing present | Attempt present |
| --- | --- | --- |
| 0 | No | No |
| 1 | Yes | No |
| 2 | No | Yes |
| 3 | Yes | Yes |

Raw bytes avoid JSON-array expansion and preserve non-UTF-8 input/output.

## Operations and semantic owners

Keep the exact variant inventory in source. These groups explain where a change
belongs and which semantics must be updated together.

| Group | Representative operations | Owning explanation |
| --- | --- | --- |
| Admission | `Authenticate`, `CaptureShellEnvironment`, `ShellEnvironmentStatus` | [Channel and peer verification](../internals/host/session-host.md) |
| Ordinary process lifetime | `Spawn`, `AdoptSpawn`, `CancelSpawn`, `ArmExit`, `Stop`, `ForceStop`, `Retire` | [Host ownership](../internals/host/session-host.md), [native lifecycle](../internals/processes/native-processes.md) |
| PTY I/O | `WriteInput`, `CloseInput`, `Resize`, `Replay`, `Snapshot` | [Terminal model](../internals/terminal.md), [recovery](../internals/host/recovery.md) |
| Observation | `Observe`, `ObserveForeground` | [Evidence collection](../internals/evidence.md) |
| Attempt evidence | `AttemptMutation`, `ProbeAttempt`, `ObserveAttempt`, `ObserveInputRetirement`, `AcknowledgeAttempt` | [Mutation receipts](../internals/host/recovery.md) |
| Shutdown | `Shutdown`, attempted shutdown, `CancelShutdownAttempt`, `AcknowledgeShutdownCancellation` | [Shutdown commit boundary](../internals/host/recovery.md) |
| One-shot jobs | `Execute`, `PollExecution`, `CancelExecution`, `AcknowledgeExecution` | [Execution journal](../internals/processes/execution.md) |

Attempt context contains attempt ID, optional session, optional fencing scope,
kind, fencing token, payload digest, and acceptance time. Attempted input uses
the binary frame's attempt field. `ExecuteInSession` has a kind tag for routing
observation, but is rejected as a generic `HostAttemptMutation`; execution uses
its own job protocol.

Launch conversion revalidates domain values and retains executable resolution,
binding/root/cwd identities, capture mode, and the absolute output checkpoint.
Resolved secret bytes use a separate redacted container with byte erasure on
drop.

### Current required fields

Launch mode, capture mode, process/tree listener evidence, and foreground
environment-name completeness are required. Missing fields fail decoding; no
legacy defaults are supplied.

## Failures and bounds

`HostFailure` transports a `HostFailureKind` and a payload-free operation
context. Its registry maps the application `PortErrorKind` values, with
`Timeout` named `timed_out` on this wire. Framing failures separately distinguish
bad headers, unsupported versions, unknown kinds, excess/incorrect lengths,
truncation, invalid identifier UTF-8, invalid domain values, overflow, and JSON
decode errors. Display messages omit payloads; arbitrary chained/debug errors
should still be handled according to the owning error type.

### Frame budgets at each endpoint

The frame ceiling is computed independently by
`ProcessHostBootstrap::codec` and `host_codec`, using configured input,
environment, replay, append, screen, and execution-output bounds plus a 64 KiB
allowance. They currently differ in the control-environment term: bootstrap
uses six times environment bytes; the daemon client uses environment bytes.
Do not document a single negotiated limit or assume equality for custom
configurations. Screen expansion uses a factor of six and execution capture a
factor of four. Encoding can fail before sending when its local ceiling is
exceeded.

`AttemptFailed` retains effect certainty and the original typed host failure.
`InputRetirementObserved` carries an optional process/exit-time pair proving
session input quiescence; it never resolves Unknown certainty. These private
variants and the durable Absent receipt require matching daemon/host builds.
Older readers reject unknown variants; the product client wire is unchanged.

### Interpret response boundaries

A response's `Completed` means that operation's particular boundary, such as
input write/flush or receipt removal. It does not universally mean application
settlement, workload exit, or durable terminal history. Request correlation
does not order concurrently executed commands. Reconnection does not resend
pending mutations; see [recovery](../internals/host/recovery.md) for outcome reconciliation.

## Change and verify the protocol

Update the registry, both client/server translations, domain validation,
compatibility defaults, frame budgets, and persistence representations affected
by a change. Keep private wire and persisted schema changes explicit. Source
tests round-trip binary execution output, raw frames, prepared identities,
capture defaults, and optional listener evidence, and reject trailing/oversized
frames and invalid stop deadlines. Host response-loss tests validate semantics
that a codec round trip cannot establish. No SDK should construct UCHS frames
as a supported integration strategy.

[Crate documentation](../README.md)
