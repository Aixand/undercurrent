# undercurrent-wire maintainer documentation

Start with [the in-memory example](development/contributing.md#run-the-example)
if you are new to the codecs. This internal crate owns bounded setup and frame
encoding. The adapter owns I/O and the product protocol owns payload meaning.
See the [crate overview](../README.md) for supported consumer entry points.

## Package and reader scope

The [architecture policy](../../../tests/architecture/allowed-graph.json)
assigns this crate `internal_ucmp_uchs_primitives`. Its exports support
workspace composition. The codecs define negotiation values, identifiers, trace
context, and ordering scalars. Queues, replay journals, sessions, and durable
state belong to callers. Use the [SDK guides](../../../docs/sdk/README.md) for
consumer integration and the [version reference](reference/scalars.md#versions)
to distinguish package, connection, and encoding compatibility. The in-memory
example exercises negotiation, fragmented input, and binary stream credit
without a daemon.

## Internals

| Question | Read |
| --- | --- |
| Which layer owns my change, buffer, or resource limit? | [Wire boundary and source map](internals/ownership.md) |
| How does a caller handle partial input and a decoded message? | [Decoding and handoff](internals/flows.md) |
| How are connection terms selected, and what does acceptance prove? | [Connection negotiation](internals/negotiation.md) |

## Reference

| Question | Read |
| --- | --- |
| Which bytes, message codes, payload rules, or errors should I expect? | [UCMP and UCHS frame layouts](reference/wire-format.md) |
| Which IDs, names, counters, cursors, trace fields, and versions are valid? | [Scalar validation and version scope](reference/scalars.md) |

## Development

[Exercise, diagnose, and change the codecs](development/contributing.md) gives
the runnable example, a failure lookup, and extension steps. Use the
[focused codec tests](development/contributing.md#verify-a-change) for a change
to encodings, validation, or negotiation.

For full product flows, follow [creation, output, and recovery](../../../docs/architecture/flows.md).
For process and dependency ownership, use [system architecture](../../../docs/architecture/README.md)
and the [workspace code map](../../../docs/architecture/dependencies.md).

[Product documentation](../../../docs/README.md)
