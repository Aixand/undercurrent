# undercurrent-wire maintainer documentation

Start with [the in-memory example](development/contributing.md#run-the-example)
if you are new to the codecs. The [crate overview](../README.md) explains this
internal library's scope and points SDK consumers to the supported interfaces.

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
the runnable example, a failure lookup, extension steps, and focused checks.

For full product flows, follow [creation, output, and recovery](../../../docs/architecture/flows.md).
For process and dependency ownership, use [system architecture](../../../docs/architecture/README.md)
and the [workspace code map](../../../docs/architecture/dependencies.md).

[Product documentation](../../../docs/README.md)
