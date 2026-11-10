# Integrate with the async Python client

This reading path points to the maintained Python guides. Python-specific setup,
method behavior, and recovery have one home here; maintained example sources
and implementation details remain in the Python package.
Use [SDK choices](../README.md) to compare supported Rust and Python surfaces.

Start with the [Linux quickstart](quickstart.md): prepare the
Python environment and matching native executables, start an isolated daemon,
verify an application acknowledgement, and clean up the fixture.

## See session control in action

Start a process, send input, verify its response, and stop it using the async client.

![Python SDK recording: start a process, send hello from Python, read its application acknowledgement, and stop the session](../../assets/demos/python-control.gif)

`APP_ACK` comes from the sample process after it reads the input; terminal echo
alone would not establish that the application processed it.
[Still image](../../assets/demos/python-control-poster.png) ·
[Recording source](../../assets/demos/fixtures/control.py) ·
[Run the complete tutorial](quickstart.md)

For a workflow with several processes, [watch the orchestration demo](../../assets/demos/orchestration.gif):
two independent jobs must exit successfully before a report session starts.
The [coordinator source](../../assets/demos/fixtures/orchestrate.py) uses a shared
client, concurrent starts, bounded waits, and explicit exit-code checks.
[Setup and verification](../../assets/demos/README.md#coordinate-jobs) explain the fixture.

## Select connection and lifetime

[Connect and share a client](operations.md#connect-and-share-a-client)
owns home/executable selection, defaults, event-loop sharing, and client lifetime.
[Python implementation](../../../python/docs/README.md#connection-invariants)
explains subprocess ownership and request/stream correlation for maintainers.

## Identify, control, and observe

| Task | Authoritative guide |
| --- | --- |
| Start, find, inspect, send input, and stop | [Operations and returned values](operations.md) |
| Wait for a state or stable observation | [State predicates and stability](state.md) |
| Consume watch updates or session events | [Streams](streams.md) |
| Distinguish input receipt from application success | [Input contract](operations.md#deliver-text-or-keys), [input_roundtrip.py](../../../python/examples/input_roundtrip.py) |

## Read complete retained intervals

[Read retained output](streams.md#read-retained-output) owns
page continuation, digest checks, retention gaps, and encoded-response bounds.
It links the maintained [read_output.py](../../../python/examples/read_output.py)
copier and its checkpoint/error behavior. Use that guide when implementing a
transcript consumer rather than inferring history from a current screen.

## Close observations and recover errors

[Python recovery](recovery.md) owns bridge failure, local
cancellation, deadlines, exceptions, and reconciliation. The shared
[recovery guide](../recovery.md) explains identities and unknown operation outcomes
across both languages. [Stream cleanup](streams.md#backpressure-and-connection-recovery)
owns iterator closure and acknowledgement placement.

[SDK choices](../README.md) · [Python package overview](../../../python/README.md)
