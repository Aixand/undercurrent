# Find the implemented local IPC design

Current public connections use protected local IPC. Follow these explanations
for the implemented boundaries, ordering, and recovery contracts.

- [Runtime ownership](README.md) distinguishes clients,
  daemon, private host and storage.
- [Local transport architecture](../../crates/undercurrent-transport-local/docs/internals/ownership.md)
  owns endpoints, negotiation and connection service loops.
- [Wire reference](../../crates/undercurrent-wire/docs/reference/wire-format.md) owns public
  framing, distinct from private host UCHS.
- [Configuration](../guides/configuration/README.md) explains home selection and client setup.
- [SDK recovery](../sdk/recovery.md) separates reconnection from mutation retry.

Current implementation and native verification remain distinct; consult
[platform coverage](../development/platform-testing.md) and the
[compatibility reference](../reference/compatibility.md) for the scope of the
current implementation.

[Documentation index](../README.md)
