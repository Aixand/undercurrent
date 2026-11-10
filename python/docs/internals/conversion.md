# Trace Python request and observation conversion

Use this explanation to locate the Python SDK 0.1.0-beta part of session creation,
input delivery, output reads, and state waits. The linked shared walkthroughs
own daemon commits and process effects; the Python wrapper owns request
construction and conversion.

## Create a session

`Client.start` constructs the product request, including Python's native-launch,
profile, and restart defaults, then `_request` verifies the returned operation
before model conversion. [Operations](../../../docs/sdk/python/operations.md#start-and-identify-a-process)
owns caller-visible defaults; the [bridge guide](../../../crates/undercurrent-python-bridge/docs/internals/operation-flow.md#session-creation-durable-success-before-response-delivery)
owns mutation metadata preparation and Rust worker execution.

Python sends `profile_binding="fixed"` by default, including when `profile=None`.
Callers can select `profile_binding="foreground_adaptive"`; invalid modes fail
before bridge IO. This compatibility default differs from the
[CLI foreground default](../../../docs/architecture/decisions/0002-cli-foreground-default.md).
The [domain profile guide](../../../crates/undercurrent-domain/docs/internals/observations-and-profiles.md#base-profile-and-effective-profile-have-different-lifetimes)
owns the distinction between durable binding and effective-profile selection.

The shared [start walkthrough](../../../docs/architecture/flows.md#start-a-session)
owns daemon commits and process effects. Python does not supply the richer
caller-fenced dispatch contract, so an idempotency key alone does not establish
restart-safe start replay. [Python recovery](../../../docs/sdk/python/recovery.md#reconcile-a-mutation)
explains reconciliation through a saved name/ID.

## Deliver input

Python constructs the ordered text/key sequence and sends the selected delivery
ID, revision precondition, and observation policy through the bridge. Input
conversion tests in [test_input.py](../../tests/test_input.py) cover this Python
boundary; they do not prove native delivery.

The application [input-delivery walkthrough](../../../crates/undercurrent-application/docs/internals/sessions/terminal-io.md#input-ordering-and-partial-success)
owns reservation, runtime writes, and later settlement, including the current
in-memory deduplication limit. Python converts the response into a receipt; the
[consumer input guide](../../../docs/sdk/python/operations.md#deliver-text-or-keys) distinguishes that
receipt from the managed application's acknowledgement.

## Trace terminal output and observations

The [shared output walkthrough](../../../docs/architecture/flows.md#consume-terminal-bytes)
owns production and daemon retention. In `Client.read_output`, Python validates
the payload digest before constructing `OutputPage`; a product gap becomes
`OutputGap`. The [output guide](../../../docs/sdk/python/streams.md#read-retained-output) owns page
continuation and the maintained copier. Screen conversion returns ANSI contents,
not the full screen DTO or a transcript.

Python adapts coalesced query watches into summary updates and yields event/gap
dictionaries for session events. On a watch gap it requests a new summary and
marks the replacement as resynchronized. The [stability algorithm](../../../docs/sdk/python/state.md#how-the-stability-timer-works)
combines watch observations with retained published events to find intermediate
contradictions; a gap invalidates continuity. Application
[event publication](../../../crates/undercurrent-application/docs/internals/recovery/events-and-outbox.md)
owns the distinction between durable outbox commits and bounded published history.

[Python maintainer documentation](../README.md)
