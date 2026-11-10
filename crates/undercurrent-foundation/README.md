# aixand-undercurrent-foundation

Use this crate when maintaining shared value validation, canonical JSON,
content digests, privacy checks, or the shared release identity to Undercurrent. It supplies synchronous,
in-memory mechanisms to domain, protocol, and storage code. It owns no daemon,
transport, database, session state, or secret resolver.

This is package **0.1.0-beta**, using Rust **1.95**, edition **2024**, with no crate
feature switches. Its `pub` exports support first-party composition;
[architecture policy](../../tests/architecture/allowed-graph.json) identifies
`aixand-undercurrent-protocol` and `aixand-undercurrent` as the supported external Rust
surfaces. Foundation supplies the shared `0.1.0-beta` release marker; each codec owns its encoding.

| Reader | Start here |
| --- | --- |
| Maintainer | [Implementation, contracts, and development guide](docs/README.md) |
| SDK integrator | [Blocking Rust SDK](../undercurrent-sdk/README.md), [protocol](../undercurrent-protocol/README.md), or [async Python package](../../python/README.md). These APIs choose the product contracts around Foundation's values. |
| Product user | [Undercurrent overview and CLI entry point](../../README.md). Foundation has no standalone install or user-facing commands. |

Start with the [in-memory example walkthrough](docs/development/contributing.md#run-the-example)
to check canonical JSON, a digest, bounded decoding, a URI, and a sensitive
diagnostic. It includes setup, expected output, and failure checks; no daemon
or runtime state is required. The [maintainer documentation](docs/README.md)
maps each mechanism to its contracts, owning source, and tests.

[NOTICE](NOTICE) records the mechanisms' original source attribution and the
deliberately excluded APIs.
