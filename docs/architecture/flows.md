# Trace creation, output, and recovery

Use these focused walkthroughs to follow an externally visible operation across
its owners. Read [process ownership](README.md) first. These diagrams
describe representative paths, not a universal transaction or retry mechanism.

## Start a session

The [finite adapter](../../crates/undercurrent-daemon/src/application_adapter/finite.rs)
converts and authorizes a request. The application start service expects that
trusted boundary; it does not independently provide a general authorization
port. Public transport admission precedes this application sequence and can
reject before dispatch.

```mermaid
%%{init: {"sequence": {"width": 120, "actorMargin": 16, "wrap": true, "wrapPadding": 4, "messageMargin": 35}}}%%
sequenceDiagram
    accTitle: Public start and separate activation commits
    accDescr: A validated request reaches application start. Creation is committed before spawn, exact ownership before binding and exit arming, and Running afterward. A response can be lost after success and post-spawn failure needs exact compensation.
    participant C as CLI or SDK
    participant D as Daemon<br/>adapter
    participant A as Start<br/>service
    participant S as SQLite<br/>repository
    participant R as Runtime and<br/>private host
    C->>D: Start request over<br/>public local IPC
    D->>A: Authorized,<br/>validated<br/>configuration
    A->>R: Prepare launch
    Note over A: Allocate<br/>session<br/>identity and<br/>retain<br/>mutation<br/>lease
    A->>S: Commit Starting<br/>and created outbox<br/>event
    A->>R: Admitted spawn
    R-->>A: Exact process<br/>incarnation
    alt Activation succeeds
        A->>S: Commit exact<br/>process ownership<br/>and event
        A->>A: Bind exact process<br/>in directory
        A->>R: Arm retained exit<br/>publication
        A->>S: Commit Running and<br/>event
        A-->>D: Session result
        D-->>C: Response may<br/>arrive or be lost
    else Failure after process<br/>effect
        A->>R: Compensate exact<br/>process when safe
        Note over A,S: Preserve<br/>ownership and<br/>uncertainty if<br/>cleanup is<br/>unconfirmed
        A-->>D: Stage-specific<br/>failure and<br/>recovery evidence
    end
```

*Takeaway: the child can have effects before the Running commit and before the
caller receives a response.* The diagram abbreviates separate durable attempt
admission/settlement around runtime effects. Dispatched starts additionally
settle their semantic result after activation, and ambiguous final settlement
preserves the live process. Ordinary Python start does not expose that complete
dispatch replay contract.

[start.rs](../../crates/undercurrent-application/src/commands/start.rs) and
[activation.rs](../../crates/undercurrent-application/src/commands/activation.rs)
own ordering. [Session transactions](../../crates/undercurrent-store-sqlite/docs/internals/transactions/sessions-and-outbox.md)
couple each aggregate mutation and its events, not the whole OS sequence.
Binding before exit arming prevents a fast exit from being published without
an identifiable owner. Projection failure after commit is diagnosed without
turning durable success into a false failure. Failure details and dispatched
replay branches belong to [session lifecycle](../../crates/undercurrent-application/docs/internals/sessions/lifecycle.md).

Investigate [command tests](../../crates/undercurrent-application/src/commands/command_tests.rs)
for `start_commits_running_session_binds_process_and_projects_summary`,
`projection_failure_does_not_report_false_failure_after_durable_start`, and
`dispatched_start_recovers_response_loss_after_settlement_commit_without_cleanup_or_respawn`.
The last test does not establish generic retry safety for every SDK start.

## Consume terminal bytes

Terminal byte offsets identify intervals within a session's output source.
They are distinct from stream frame sequences and session-event cursors.
The next diagram focuses on finite reads so the delivered interval is explicit.

```mermaid
%%{init: {"sequence": {"width": 120, "actorMargin": 16, "wrap": true, "wrapPadding": 4, "messageMargin": 35}}}%%
sequenceDiagram
    accTitle: Terminal production and bounded finite paging — production
    accDescr: The managed child writes PTY bytes to its private host, which sends ordered output to the daemon runtime and bounded output store.
    participant P as Managed<br/>child
    participant H as Session host
    participant R as Hosted<br/>runtime
    participant O as Bounded<br/>output store
    P-->>H: PTY bytes
    H-->>R: Private output<br/>frame and source<br/>offset
    R->>O: Append ordered<br/>bytes and update<br/>screen projection
```

*Production: private host output is distinct from the public read boundary.*

```mermaid
%%{init: {"sequence": {"width": 120, "actorMargin": 16, "wrap": true, "wrapPadding": 4, "messageMargin": 35}}}%%
sequenceDiagram
    accTitle: Terminal production and bounded finite paging — finite read
    accDescr: An SDK reads from its checkpoint through the authorized adapter. It processes the delivered interval before checkpointing, or handles an explicit retention gap.
    participant O as Bounded<br/>output store
    participant D as Read adapter
    participant C as SDK consumer
    C->>D: Read from saved<br/>byte offset
    D->>O: Authorized bounded<br/>retained read
    alt Requested prefix retained
        O-->>D: Delivered interval<br/>and producer head
        D-->>C: Data page and<br/>digest
        C->>C: Validate, process,<br/>then checkpoint<br/>delivered end
    else Requested prefix evicted
        O-->>D: Available starting<br/>offset
        D-->>C: Explicit retention<br/>gap
        C->>C: Fail complete<br/>capture or record<br/>intentional loss
    end
```

*Finite read: processing the returned interval and recording loss are consumer responsibilities.*

*Takeaway: a producer head can be beyond the returned page. Advancing directly
to it can skip bytes that are still available.* The authoritative implementation
is [terminal_output.rs](../../crates/undercurrent-application/src/terminal_output.rs),
with DTO mapping in [feed.rs](../../crates/undercurrent-daemon/src/presentation/feed.rs).
Rust product pages expose `to_offset`; Python currently omits it, so the
maintained [copy example](../../python/examples/read_output.py) validates and
uses `from_offset + len(data)`. The [acknowledgment example](../../python/examples/input_roundtrip.py)
uses the same interval validator and retains only the suffix needed to match
across contiguous pages. Neither joins across a gap silently.

Live output adds bounded stream admission, credit, scheduling, write-confirmed
replay and consumer checkpoints. See [daemon streams](../../crates/undercurrent-daemon/docs/internals/output-and-streams.md),
[transport delivery](../../crates/undercurrent-transport-core/docs/internals/streams-and-recovery.md),
and [multiplex replay](../../crates/undercurrent-multiplex/docs/internals/replay.md).
A socket write is not application consumption; an output digest verifies a
payload, not completeness of the transcript. The CLI's current `logs --follow`
seed limitation is recorded in [output tasks](../guides/cli/status-and-output.md).

## Recover after daemon replacement

The instance lock and coupled-file checks precede recovery. Durable sessions
are scanned in stable-ID pages. Runtime truth is observed outside the session
lease and revalidated after acquiring it, so recovery cannot overwrite a newer
incarnation merely because it observed an older process first.

```mermaid
%%{init: {"sequence": {"width": 120, "actorMargin": 16, "wrap": true, "wrapPadding": 4, "messageMargin": 35}}}%%
sequenceDiagram
    accTitle: Reconcile sessions before opening public service
    accDescr: Composition locks its home and opens storage, then observes exact process truth, revalidates under a session lease, and rebuilds state. Unknown process ownership remains unresolved. Attempt readiness is separately evaluated before admitting runtime mutations.
    participant D as Daemon<br/>composition
    participant S as SQLite<br/>repository
    participant A as Startup<br/>reconciliation
    participant H as Runtime and<br/>session host
    D->>D: Acquire instance<br/>lock and verify<br/>coupled files
    D->>S: Open and validate<br/>durable state
    D->>A: Reconcile ordered<br/>session pages
    A->>S: Read durable session<br/>and incarnation
    A->>H: Observe exact<br/>process truth
    A->>A: Acquire lease,<br/>reload, revalidate<br/>owner
    alt Truth supports<br/>reconciliation
        A->>S: Commit required<br/>lifecycle changes
        A->>A: Restore exact<br/>bindings,<br/>projections,<br/>schedules
    else Orphaned or unknown<br/>process truth
        A-->>D: Retain unresolved<br/>issue, readiness<br/>closed
    end
    A-->>D: Scan completeness<br/>and unresolved count
    Note over D: Only ready<br/>session<br/>recovery<br/>permits<br/>worker<br/>startup
    D->>D: Evaluate separate<br/>attempt<br/>mutation-readiness<br/>gate
    D->>D: Bind public service<br/>and retain query<br/>access as allowed
```

*Takeaway: recovery reconstructs from evidence. It neither infers identity from
a PID alone nor retries every uncertain process action.* Terminal sessions can
re-enqueue durable restart intent. Stopping with no-escalation or a claimed
Unknown cancellation can be preserved without repeating termination. A ready
session scan does not mean every cancellation result is known.

The exact decision branches and bounded revalidation are in
[startup recovery](../../crates/undercurrent-application/docs/internals/recovery/startup.md).
[Host recovery](../../crates/undercurrent-runtime/docs/internals/host/recovery.md) owns receipts,
epochs, replay files, and missing-proof interpretation. A replacement host in
one adapter can reuse its configured epoch; do not infer a fresh epoch from
every host restart. Current background replay failures are not a promise of
perpetual retry.

Relevant evidence includes [startup tests](../../crates/undercurrent-daemon/tests/startup_recovery.rs),
[host recovery tests](../../crates/undercurrent-daemon/tests/hosted_process_recovery.rs),
and application tests that a second startup never redrives a previously claimed
force cancellation. [SDK recovery](../sdk/recovery.md) explains what callers can
actually reconcile after response loss.

[Extension guide](../development/extending.md) · [Documentation index](../README.md)
